import cron, { type ScheduledTask } from 'node-cron';
import type { Agent } from '@mastra/core/agent';
import {
  type Schedule,
  type ChangeEvent,
  getSchedule,
  getLastRun,
  listSchedules,
  onSchedulesChange,
  putLastRun,
  deleteLastRun,
} from './redis';
import { getCached, rewriteSlashDispatch } from './skills';
import { CRON_RESOURCE_ID, CRON_THREAD_PREFIX } from './memory-cleanup';

const OUTPUT_TRUNCATE_BYTES = 8 * 1024;

const jobs = new Map<string, ScheduledTask>();
const running = new Set<string>();

let agent: Agent | null = null;

export function isValidCron(expr: string): boolean {
  return cron.validate(expr);
}

function unregister(name: string): void {
  const job = jobs.get(name);
  if (!job) return;
  job.stop();
  jobs.delete(name);
}

function register(name: string, schedule: Schedule): void {
  unregister(name);
  if (!isValidCron(schedule.cron)) {
    console.error(`[skillit] invalid cron "${schedule.cron}" for skill ${name}; not scheduling`);
    return;
  }
  const task = cron.schedule(
    schedule.cron,
    () => { void runScheduled(name, schedule); },
    schedule.tz ? { timezone: schedule.tz } : undefined,
  );
  jobs.set(name, task);
  console.log(`[skillit] scheduled "${name}" at "${schedule.cron}"${schedule.tz ? ` (${schedule.tz})` : ''}`);
}

async function runScheduled(name: string, schedule: Schedule): Promise<void> {
  if (running.has(name)) {
    console.warn(`[skillit] skipping scheduled run of "${name}" — previous run still in flight`);
    return;
  }
  running.add(name);
  const startedAt = Date.now();
  const ranAt = new Date(startedAt).toISOString();

  try {
    if (!getCached(name)) {
      await putLastRun(name, {
        ranAt, durationMs: 0,
        error: `Skill "${name}" not found in cache when schedule fired`,
      });
      console.warn(`[skillit] scheduled fire for "${name}" but skill is missing`);
      return;
    }
    if (!agent) {
      const err = `agent not initialised`;
      await putLastRun(name, { ranAt, durationMs: 0, error: err });
      console.error(`[skillit] scheduled fire for "${name}" but ${err}`);
      return;
    }

    // Apply the same /<skill> rewrite SlashDispatchAdapter does for chat, so
    // the LLM gets the skill content inlined and the admin prompt directly
    // — no run_skill tool round-trip needed. The prompt is optional; when
    // empty, fire the skill with no additional input.
    const adminPrompt = (schedule.prompt ?? '').trim();
    const slashPrompt = adminPrompt ? `/${name} ${adminPrompt}` : `/${name}`;
    const prompt = rewriteSlashDispatch(slashPrompt) ?? slashPrompt;
    const conversationId = `${CRON_THREAD_PREFIX}${name}:${startedAt}`;

    const result = await agent.generate(prompt, {
      memory: { thread: conversationId, resource: CRON_RESOURCE_ID },
    });
    const output = (result.text ?? '').trim();
    const truncated = output.length > OUTPUT_TRUNCATE_BYTES
      ? output.slice(0, OUTPUT_TRUNCATE_BYTES) + `\n…[truncated ${output.length - OUTPUT_TRUNCATE_BYTES} bytes]`
      : output;

    await putLastRun(name, {
      ranAt,
      durationMs: Date.now() - startedAt,
      ...(truncated ? { output: truncated } : {}),
    });
    console.log(`[skillit] scheduled run of "${name}" completed in ${Date.now() - startedAt}ms`);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await putLastRun(name, {
      ranAt,
      durationMs: Date.now() - startedAt,
      error: message,
    });
    console.error(`[skillit] scheduled run of "${name}" failed:`, message);
  } finally {
    running.delete(name);
  }
}

async function applyChange(event: ChangeEvent): Promise<void> {
  if (event.op === 'delete') {
    unregister(event.name);
    await deleteLastRun(event.name);
    return;
  }
  const schedule = await getSchedule(event.name);
  if (schedule) register(event.name, schedule);
}

export async function initScheduler(theAgent: Agent): Promise<void> {
  agent = theAgent;

  const schedules = await listSchedules();
  for (const [name, schedule] of Object.entries(schedules)) register(name, schedule);
  onSchedulesChange((event) => {
    applyChange(event).catch((err) =>
      console.error('[skillit] failed to apply schedule change', event, err),
    );
  });
  console.log(`[skillit] scheduler started with ${jobs.size} job(s)`);
}

/**
 * Manually fire a skill's scheduled run on demand (used by the admin
 * "Run now" button). Uses the saved schedule's prompt.
 */
export async function runNow(name: string): Promise<void> {
  const schedule = await getSchedule(name);
  if (!schedule) throw new Error(`No schedule configured for "${name}"`);
  await runScheduled(name, schedule);
}

export async function lastRunFor(name: string) {
  return getLastRun(name);
}
