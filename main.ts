// =================================================================
// The "Singularity" Research Bot v13.0 - The Self-Improving Agent
//
// This version uses an external database (Supabase) to overcome
// storage limits and implements a learning/anti-stagnation protocol
// to ensure it continuously improves and never gets stuck.
// =================================================================

import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

// --- BOT CONFIGURATION & CONSTANTS ---
const BROWSER_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36';
const FORCED_EVOLUTION_THRESHOLD = 3; // Trigger anti-stagnation after 3 failures
const SEED_SITE_FOR_EVOLUTION = "https://yourstory.com/companies"; // A reliable source of company names for the escape hatch
const VERIFICATION_TIMEOUT = 25000;
const MAX_CONTENT_LENGTH = 15000;
const PORTAL_CONFIDENCE_THRESHOLD = 0.7; // Stricter threshold for higher quality results
const DISCOVERY_BATCH_SIZE = 12; // Number of search queries per cycle

// --- INITIALIZE EXTERNAL CLIENTS ---

// Initialize Supabase client (ensure env vars are set in Deno Deploy)
const supabaseUrl = Deno.env.get('SUPABASE_URL');
const supabaseAnonKey = Deno.env.get('SUPABASE_ANON_KEY');
if (!supabaseUrl || !supabaseAnonKey) {
  throw new Error("SUPABASE_URL and SUPABASE_ANON_KEY environment variables are required.");
}
const supabase = createClient(supabaseUrl, supabaseAnonKey);

// --- ESSENTIAL HELPER FUNCTIONS ---

async function safeFetch(url: string, timeoutMs: number, options: RequestInit = {}): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
      headers: { 'User-Agent': BROWSER_USER_AGENT, ...options.headers }
    });
    return response;
  } catch (error) {
    if (error.name === 'AbortError') throw new Error(`Request timed out after ${timeoutMs}ms`);
    throw error;
  } finally {
    clearTimeout(timeoutId);
  }
}

async function callGeminiWithFallback(prompt: string, maxRetries: number = 3): Promise<any> {
    const keys = (Deno.env.get("GEMINI_API_KEYS") || "").split(',').map(k => k.trim()).filter(Boolean);
    if (keys.length === 0) throw new Error("GEMINI_API_KEYS is not set.");

    for (let attempt = 0; attempt < maxRetries; attempt++) {
        const key = keys[attempt % keys.length];
        try {
            const res = await safeFetch(
                `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key=${key}`,
                30000,
                {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        contents: [{ parts: [{ text: prompt }] }],
                        generationConfig: { temperature: 0.3 + (attempt * 0.1), response_mime_type: "application/json" }
                    })
                }
            );
            if (res.ok) {
                const result = await res.json();
                if (result.candidates && result.candidates.length > 0) {
                    const textContent = result.candidates[0].content.parts[0].text;
                    return JSON.parse(textContent);
                }
            }
        } catch (e) {
            console.warn(`⚠️ Gemini attempt ${attempt + 1} failed: ${e.message}`);
        }
    }
    throw new Error("All Gemini API attempts failed.");
}

async function runMultiThreadedDiscovery(queries: string[]): Promise<any[]> {
    const serpApiKeys = (Deno.env.get("SERPAPI_KEYS") || "").split(',').map(k => k.trim()).filter(Boolean);
    if (serpApiKeys.length === 0) throw new Error("SERPAPI_KEYS is not set.");

    const promises = queries.map(async (query, index) => {
        const apiKey = serpApiKeys[index % serpApiKeys.length];
        try {
            const searchUrl = `https://serpapi.com/search.json?q=${encodeURIComponent(query)}&api_key=${apiKey}&google_domain=google.co.in&gl=in`;
            const searchRes = await safeFetch(searchUrl, 20000);
            if (searchRes.ok) {
                const data = await searchRes.json();
                if (data.organic_results) {
                    return { query, results: data.organic_results, success: true };
                }
            }
        } catch (error) {
            console.warn(`⚠️ Search failed for "${query}": ${error.message}`);
        }
        return { query, results: [], success: false };
    });
    return Promise.allSettled(promises);
}

function preprocessWebContent(content: string): string {
    return content
        .replace(/<script[\s\S]*?<\/script>/gi, '')
        .replace(/<style[\s\S]*?<\/style>/gi, '')
        .replace(/<!--[\s\S]*?-->/g, '')
        .replace(/<[^>]+>/g, ' ')
        .replace(/\s+/g, ' ')
        .trim()
        .substring(0, MAX_CONTENT_LENGTH);
}

async function runAdvancedVerifier(url: string): Promise<any> {
    try {
        const response = await safeFetch(url, VERIFICATION_TIMEOUT);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);

        const processedContent = preprocessWebContent(await response.text());
        const verificationPrompt = `Analyze this webpage to determine if it's a legitimate job portal or company careers page.
URL: ${url}
Content: "${processedContent}"
Respond ONLY with valid JSON: {"is_job_portal": true/false, "confidence_score": 0.0-1.0, "reasoning": "brief explanation"}`;

        return await callGeminiWithFallback(verificationPrompt);
    } catch (error) {
        console.warn(`⚠️ Verification failed for ${url}: ${error.message}`);
        return { is_job_portal: false, confidence_score: 0 };
    }
}

// --- PILLAR 1: STRATEGIC LEARNING ---

async function getBotState() {
    const { data, error } = await supabase.from('bot_state').select('*').eq('id', 1).single();
    if (error) {
        console.error("Fatal: Could not fetch bot state. Check if the database is initialized.", error);
        throw new Error(`Could not fetch bot state: ${error.message}`);
    }
    return data;
}

async function getWinningPatterns(): Promise<string[]> {
    const { data, error } = await supabase
        .from('career_portals')
        .select('discovery_query')
        .order('last_verified_at', { ascending: false })
        .limit(50);
    if (error) {
        console.warn("Could not fetch winning patterns, will rely on exploration.");
        return [];
    }
    return data.map(item => item.discovery_query).filter(Boolean);
}

// --- PILLAR 2: DYNAMIC EXECUTION ---

async function generateAdaptiveQueries(winningPatterns: string[], consecutiveFailures: number): Promise<string[]> {
    const exploreRatio = consecutiveFailures > 0 ? 0.7 : 0.2;
    const exploitCount = Math.floor(DISCOVERY_BATCH_SIZE * (1 - exploreRatio));
    const exploreCount = DISCOVERY_BATCH_SIZE - exploitCount;

    console.log(`Generating queries -> Strategy: ${exploitCount} Exploit, ${exploreCount} Explore`);

    const exploitPrompt = `Generate ${exploitCount} search queries to find Indian company career pages based on these proven examples:\n${winningPatterns.slice(0, 10).join('\n')}\nReturn as JSON: {"queries":[]}`;
    const explorePrompt = `Generate ${exploreCount} creative and unconventional search queries to discover Indian tech company career pages. Think outside the box: search VC portfolios, tech blogs, or developer forums. Return as JSON: {"queries":[]}`;

    const [exploitResult, exploreResult] = await Promise.all([
        exploitCount > 0 ? callGeminiWithFallback(exploitPrompt) : Promise.resolve({ queries: [] }),
        exploreCount > 0 ? callGeminiWithFallback(explorePrompt) : Promise.resolve({ queries: [] })
    ]);

    const queries = [...(exploitResult.queries || []), ...(exploreResult.queries || [])];
    console.log(`Generated ${queries.length} adaptive queries.`);
    return queries;
}

// --- PILLAR 3: ANTI-STAGNATION PROTOCOL ---

async function runForcedEvolution(): Promise<string[]> {
    console.warn("🚨 ANTI-STAGNATION PROTOCOL TRIGGERED: Running Forced Evolution cycle.");
    console.log(`Executing Seed Crawl on: ${SEED_SITE_FOR_EVOLUTION}`);

    try {
        const response = await fetch(SEED_SITE_FOR_EVOLUTION);
        const textContent = await response.text();
        const prompt = `From the following webpage content, extract up to 15 company names. Return as JSON: {"companies": ["name1", "name2", ...]}.\n\nCONTENT: ${textContent.substring(0, MAX_CONTENT_LENGTH)}`;
        const llmResult = await callGeminiWithFallback(prompt);

        if (!llmResult.companies || llmResult.companies.length === 0) throw new Error("Failed to extract company names.");

        console.log(`Extracted ${llmResult.companies.length} company names for direct searching.`);
        return llmResult.companies.map((name: string) => `"${name}" careers page`);
    } catch (error) {
        console.error(`Forced Evolution failed: ${error.message}. Using fallback query.`);
        return ["Indian tech startup careers page 2024"];
    }
}

// --- CORE ORCHESTRATION & DATABASE LOGIC ---

async function processAndSaveResults(discoveredUrls: Set<string>, discoveryQueryMap: Map<string, string>): Promise<number> {
    const { data: existingPortals, error } = await supabase.from('career_portals').select('hostname');
    if (error) throw new Error("Could not fetch existing portals for filtering.");
    const existingHostnames = new Set(existingPortals.map(p => p.hostname));

    const urlsToVerify = [...discoveredUrls].filter(url => {
        try {
            return !existingHostnames.has(new URL(url).hostname);
        } catch { return false; }
    });

    console.log(`Found ${urlsToVerify.length} new, unique URLs to verify.`);
    if (urlsToVerify.length === 0) return 0;

    let newPortalsFound = 0;
    for (const url of urlsToVerify) {
        const verification = await runAdvancedVerifier(url);
        if (verification.is_job_portal && verification.confidence_score >= PORTAL_CONFIDENCE_THRESHOLD) {
            const hostname = new URL(url).hostname;
            const { error } = await supabase.from('career_portals').insert({
                url, hostname,
                discovery_query: discoveryQueryMap.get(url),
                last_verified_at: new Date().toISOString()
            });

            if (error) {
                if (error.code !== '23505') { // 23505 is unique violation, which is okay
                    console.warn(`Failed to save ${url}: ${error.message}`);
                }
            } else {
                console.log(`✅ SAVED TO DB: ${url}`);
                newPortalsFound++;
                existingHostnames.add(hostname);
            }
        }
    }
    return newPortalsFound;
}

async function runSmarterBotCycle() {
    console.log("\n🚀 === Starting Smarter Bot Cycle ===");
    const state = await getBotState();
    let queries: string[];

    if (state.consecutive_failures >= FORCED_EVOLUTION_THRESHOLD) {
        queries = await runForcedEvolution();
        await supabase.from('bot_state').update({ last_run_strategy: 'Forced Evolution' }).eq('id', 1);
    } else {
        const patterns = await getWinningPatterns();
        queries = await generateAdaptiveQueries(patterns, state.consecutive_failures);
        await supabase.from('bot_state').update({ last_run_strategy: 'Adaptive' }).eq('id', 1);
    }

    if (queries.length === 0) {
        console.warn("No queries were generated. Ending cycle early.");
        return;
    }

    const discoveryResults = await runMultiThreadedDiscovery(queries);
    const discoveredUrls = new Set<string>();
    const discoveryQueryMap = new Map<string, string>();
    discoveryResults.forEach((res: any) => {
        if (res.status === 'fulfilled' && res.value?.success) {
            res.value.results.forEach((item: any) => {
                if (item.link && item.link.startsWith('http')) {
                    discoveredUrls.add(item.link);
                    discoveryQueryMap.set(item.link, res.value.query);
                }
            });
        }
    });

    const newPortalsFound = await processAndSaveResults(discoveredUrls, discoveryQueryMap);
    const newFailureCount = newPortalsFound === 0 ? state.consecutive_failures + 1 : 0;
    await supabase.from('bot_state').update({ consecutive_failures: newFailureCount }).eq('id', 1);

    console.log("📊 === Cycle Summary ===");
    console.log(`New portals this cycle: ${newPortalsFound}`);
    console.log(`Consecutive failures: ${newFailureCount}`);
    console.log("=".repeat(50));
}

// --- DENO DEPLOY ENTRYPOINTS ---

Deno.cron("Smarter Job Bot", "*/8 * * * *", () => {
    console.log("⏰ Smarter Bot triggered by cron.");
    runSmarterBotCycle().catch(console.error);
});

Deno.serve(async (req) => {
    const url = new URL(req.url);
    if (url.pathname === '/trigger') {
        console.log("👋 Manual Smarter Bot trigger received.");
        runSmarterBotCycle().catch(console.error);
        return new Response("Smarter Bot cycle triggered! Check logs for progress.");
    }
     if (url.pathname === '/reset-failures') {
        await supabase.from('bot_state').update({ consecutive_failures: 0 }).eq('id', 1);
        return new Response("Failure count has been reset.");
    }
    return new Response("Bot is running. Use /trigger to run a cycle manually or /reset-failures to reset the failure counter.");
});
