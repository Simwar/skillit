import { createTool } from '@mastra/core/tools';
import { z } from 'zod';
import { tavily } from '@tavily/core';

export const webSearchTool = createTool({
  id: 'web_search',
  description:
    'Search the web for up-to-date information. Use this when a skill requires current facts, news, prices, or anything that may have changed since the model\'s training cutoff.',
  inputSchema: z.object({
    query: z.string().describe('Search query.'),
    max_results: z.number().int().min(1).max(10).optional().default(5).describe('Number of results to return (default 5).'),
  }),
  execute: async ({ query, max_results }) => {
    const apiKey = process.env.TAVILY_API_KEY;
    if (!apiKey) return { ok: false, error: 'TAVILY_API_KEY is not configured.' };

    const client = tavily({ apiKey });
    const { results } = await client.search(query, {
      maxResults: max_results,
      includeAnswer: true,
    });

    return {
      ok: true,
      results: results.map((r: { title: string; url: string; content: string }) => ({
        title: r.title,
        url: r.url,
        snippet: r.content,
      })),
    };
  },
});
