import cron, { type ScheduledTask } from 'node-cron';
import type { Agent } from '@mastra/core/agent';
import {
  MessagingClient,
  type ConversationStream,
} from '@astropods/messaging';
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
const SLACK_BODY_MAX_CHARS = 35_000;

const jobs = new Map<string, ScheduledTask>();
const running = new Set<string>();

let agent: Agent | null = null;
let client: MessagingClient | null = null;
let clientReady: Promise<void> | null = null;
let conv: ConversationStream | null = null;

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
  const slackTag = schedule.slackChannel ? ` → ${schedule.slackChannel}` : '';
  console.log(`[skillit] scheduled "${name}" at "${schedule.cron}"${schedule.tz ? ` (${schedule.tz})` : ''}${slackTag}`);
}

/**
 * Push a proactive AgentResponse to the messaging service with a
 * conversation_id the Slack adapter recognises (channel id or
 * `<channel>-<thread_ts>`). The service broadcasts unmatched conversation
 * ids to all adapters; the Slack adapter accepts on conversation_id
 * format and posts. END is what actually triggers the post.
 *
 * The bot must be a member of the channel — otherwise Slack returns
 * `not_in_channel` and the post is silently dropped.
 */
function postToSlack(channelId: string, body: string): void {
  if (!conv) {
    console.warn(`[skillit] cannot post to Slack channel ${channelId} — bidi stream not open yet`);
    return;
  }
  const truncated = body.length > SLACK_BODY_MAX_CHARS
    ? body.slice(0, SLACK_BODY_MAX_CHARS) + `\n…[truncated ${body.length - SLACK_BODY_MAX_CHARS} chars]`
    : body;
  conv.sendAgentResponse({
    conversationId: channelId,
    content: { type: 'REPLACE', content: truncated },
  });
  conv.sendAgentResponse({
    conversationId: channelId,
    content: { type: 'END', content: '' },
  });
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
    const durationMs = Date.now() - startedAt;

    await putLastRun(name, {
      ranAt,
      durationMs,
      ...(truncated ? { output: truncated } : {}),
    });
    console.log(`[skillit] scheduled run of "${name}" completed in ${durationMs}ms`);

    if (schedule.slackChannel && output) {
      postToSlack(
        schedule.slackChannel,
        `:robot_face: *skillit* ran \`${name}\` (${durationMs}ms)\n\`\`\`\n${output}\n\`\`\``,
      );
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const durationMs = Date.now() - startedAt;
    await putLastRun(name, {
      ranAt,
      durationMs,
      error: message,
    });
    console.error(`[skillit] scheduled run of "${name}" failed:`, message);

    if (schedule.slackChannel) {
      postToSlack(
        schedule.slackChannel,
        `:warning: *skillit* — \`${name}\` failed (${durationMs}ms)\n\`\`\`\n${message}\n\`\`\``,
      );
    }
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

  // Open a bidi conversation stream to the messaging sidecar so the scheduler
  // can push proactive AgentResponses (used for Slack posting when a schedule
  // has `slackChannel` set). The stream is opened in the background; per-call
  // gates check `conv` before posting.
  const addr = process.env.GRPC_SERVER_ADDR || 'localhost:9090';
  client = new MessagingClient(addr);
  client.on('reconnecting', (evt: { attempt: number; delayMs: number; reason?: string }) => {
    console.warn(`[skillit] messaging client reconnecting (attempt ${evt.attempt} in ${evt.delayMs}ms)${evt.reason ? `: ${evt.reason}` : ''}`);
  });
  client.on('reconnected', (evt: { attempt: number }) => {
    console.log(`[skillit] messaging client reconnected after ${evt.attempt} attempt(s)`);
  });

  clientReady = client.connectWithRetry({
    initialDelayMs: 500,
    maxDelayMs: 10_000,
    jitter: true,
  });
  clientReady
    .then(() => {
      conv = client!.createConversationStream();
      conv.on('error', (err: Error) => console.error('[skillit] bidi conversation stream error:', err));
      conv.on('reconnecting', (evt: { attempt: number; delayMs: number; reason?: string }) => {
        console.warn(`[skillit] bidi stream reconnecting (attempt ${evt.attempt} in ${evt.delayMs}ms)${evt.reason ? `: ${evt.reason}` : ''}`);
      });
      conv.on('reconnected', (evt: { attempt: number }) => {
        console.log(`[skillit] bidi stream reconnected after ${evt.attempt} attempt(s)`);
      });
      console.log('[skillit] messaging client connected; bidi conversation stream open');
    })
    .catch((err) => console.error('[skillit] messaging client connect failed permanently:', err));

  const schedules = await listSchedules();
  for (const [name, schedule] of Object.entries(schedules)) register(name, schedule);
  onSchedulesChange((event) => {
    applyChange(event).catch((err) =>
      console.error('[skillit] failed to apply schedule change', event, err),
    );
  });
  console.log(`[skillit] scheduler started with ${jobs.size} job(s) (messaging connect in background)`);
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
