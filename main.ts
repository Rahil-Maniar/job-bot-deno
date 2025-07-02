// =================================================================
// The "Singularity" Research Bot v12.0 - LLM-Native Intelligence
// Multi-threaded, intelligent portal discovery with self-evolution
// All major logic points are now driven by LLM analysis.
// =================================================================

// --- ENHANCED CONSTANTS ---
const BROWSER_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36';
const MAX_PARALLEL_VERIFICATIONS = 25;
const MAX_PARALLEL_EXPLORATIONS = 8;
const DISCOVERY_BATCH_SIZE = 15;
const PORTAL_CONFIDENCE_THRESHOLD = 0.75;

// --- DYNAMIC SEED STRATEGIES (for initial bootstrapping) ---
const SEED_EXPANSION_STRATEGIES = [
  "https://yourstory.com/jobs", "https://inc42.com/startups", "https://entrackr.com/category/startups",
  "https://analyticsindiamag.com/jobs", "https://www.kaggle.com/jobs", "https://www.ncs.gov.in"
];

// --- HELPER: Safe Fetch with Timeout ---
// Implements a robust timeout using AbortController, as the native fetch doesn't support a timeout option.
async function safeFetch(url: string, timeoutMs: number, options: RequestInit = {}): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal
    });
    return response;
  } catch (error) {
    if (error.name === 'AbortError') {
      throw new Error(`Request timed out after ${timeoutMs}ms`);
    }
    throw error;
  } finally {
    clearTimeout(timeoutId);
  }
}

// --- HELPER: Enhanced Gemini API with Retry Logic ---
async function callGeminiWithFallback(prompt: string, maxRetries: number = 3) {
  const primaryModel = "gemini-2.5-pro";
  const secondaryModel = "gemini-2.0-flash";
  const keysEnv = Deno.env.get("GEMINI_API_KEYS") || "";
  const keys = keysEnv.split(',').map(k => k.trim()).filter(Boolean);
  if (keys.length === 0) throw new Error("GEMINI_API_KEYS environment variable is not set.");

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    const key = keys[attempt % keys.length]; // Cycle through keys
    for (const model of [primaryModel, secondaryModel]) {
      try {
        const res = await safeFetch(`https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`, 20000, { // 20-second timeout
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            contents: [{ parts: [{ text: prompt }] }],
            generationConfig: {
              temperature: 0.1 + (attempt * 0.2),
              maxOutputTokens: 8192,
              responseMimeType: "application/json", // Enforce JSON output
            }
          })
        });
        if (res.ok) {
          const result = await res.json();
          if (result.candidates && result.candidates.length > 0) {
            console.log(`✅ Gemini Success: Model ${model} (Attempt ${attempt + 1})`);
            // The response from the Gemini API is now a JSON object. We need to get the text part.
            return JSON.parse(result.candidates[0].content.parts[0].text);
          }
        }
      } catch (e) {
        console.warn(`⚠️ Gemini Exception: Model ${model}, Key ending in ...${key.slice(-4)}: ${e.message}`);
      }
    }
    if (attempt < maxRetries - 1) await new Promise(resolve => setTimeout(resolve, (attempt + 1) * 1000));
  }
  throw new Error("All API keys and models failed after multiple retries.");
}


// --- LLM-DRIVEN INTELLIGENCE: Adaptive Search Query Generation ---
async function generateAdaptiveSearchQueries(analytics: any, categoryFocus: string) {
  const consecutiveFailures = analytics.consecutiveFailures || 0;
  const creativityLevel = Math.min(5, Math.floor(consecutiveFailures / 2) + 1);

  const prompt = `
You are a master market research AI specializing in Indian job markets. Your goal is to generate ${DISCOVERY_BATCH_SIZE} highly effective Google search queries to discover new job portals and company career pages.

CURRENT STATE:
- Consecutive Failures: ${consecutiveFailures}
- Creativity Level: ${creativityLevel}/5
- Focus Category: ${categoryFocus}
- Successful Past Queries (for inspiration): ${JSON.stringify(analytics.successfulPatterns?.slice(-5) || [])}
- Failed Past Queries (to avoid): ${JSON.stringify(analytics.failedPatterns?.slice(-10) || [])}

TASK: Based on the current state, generate a diverse batch of search queries. If creativity is high, think of unconventional sources like tech blogs, forums, or government tenders.

Respond with a single JSON object with one key: "queries", which is an array of strings.
Example: { "queries": ["query1", "query2", ...] }`;

  try {
    const result = await callGeminiWithFallback(prompt);
    return result.queries || [];
  } catch (error) {
    console.error("❌ Failed to generate search queries with LLM, using fallback.", error.message);
    return [`${categoryFocus} job portals india`, `top IT companies in India careers ${new Date().getFullYear()}`];
  }
}

// --- LLM-DRIVEN INTELLIGENCE: Batch Company URL Prediction ---
async function predictCompanyCareerUrls(companyNames: string[]): Promise<string[]> {
    if (companyNames.length === 0) return [];
    const prompt = `
You are an expert system that predicts official career page URLs for a list of company names.
Consider standard patterns like /careers, /jobs, /life-at-[company], jobs.[company].com, etc.

Company Names: ${JSON.stringify(companyNames)}

Respond with a single JSON object. The key should be "predicted_urls" and the value should be an array of potential URL strings. Provide 2-3 best guesses for each company.
Example: { "predicted_urls": ["https://companyA.com/careers", "https://jobs.companyB.com", ...] }`;

    try {
        const result = await callGeminiWithFallback(prompt);
        return result.predicted_urls || [];
    } catch (error) {
        console.error(`❌ LLM failed to predict company URLs: ${error.message}`);
        return [];
    }
}

// --- LLM-DRIVEN INTELLIGENCE: Portal Verifier & Classifier ---
async function runAdvancedVerifier(url: string): Promise<any> {
    try {
        const response = await safeFetch(url, 15000, { headers: { 'User-Agent': BROWSER_USER_AGENT } });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const content = await response.text();

        const verificationPrompt = `
You are an elite job portal verification specialist for Indian IT opportunities. Analyze the provided webpage content.

- Is this a legitimate job portal or a company careers page with active IT/software/data job listings?
- Assess its quality, relevance, and the number of IT jobs.

URL: ${url}
Content Snippet: """${content.substring(0, 30000)}"""

Respond with a single, valid JSON object with the following schema:
{
  "is_job_portal": boolean,
  "confidence_score": float (0.0 to 1.0),
  "has_it_jobs": boolean,
  "portal_type": "aggregator|niche_board|company_careers|other|not_a_portal",
  "portal_quality": "low|medium|high|premium",
  "it_job_count_estimate": integer,
  "reasoning": "A brief explanation for your decision.",
  "final_url": "${url}"
}`;

        const verificationData = await callGeminiWithFallback(verificationPrompt);
        verificationData.final_url = url; // Ensure final_url is always present
        return verificationData;

    } catch (error) {
        console.warn(`⚠️ Verification failed for ${url}: ${error.message}`);
        return { is_job_portal: false, confidence_score: 0, error: error.message, final_url: url };
    }
}

// --- CORE LOGIC: Multi-threaded Discovery ---
async function runMultiThreadedDiscovery(searchQueries: string[]) {
    const serpApiKeys = (Deno.env.get("SERPAPI_KEYS") || "").split(',').map(k => k.trim()).filter(Boolean);
    if (serpApiKeys.length === 0) throw new Error("SERPAPI_KEYS environment variable is not set.");

    const discoveryPromises = searchQueries.map(async (query, index) => {
        const apiKey = serpApiKeys[index % serpApiKeys.length];
        try {
            const searchRes = await safeFetch(`https://serpapi.com/search.json?q=${encodeURIComponent(query)}&api_key=${apiKey}&google_domain=google.co.in&gl=in&num=20`, 10000);
            if (searchRes.ok) {
                const data = await searchRes.json();
                if (!data.error && data.organic_results) {
                    return { query, results: data.organic_results.slice(0, 10), success: true };
                }
            }
        } catch (error) {
            console.warn(`⚠️ Search failed for "${query}": ${error.message}`);
        }
        return { query, results: [], success: false };
    });
    return await Promise.allSettled(discoveryPromises);
}

// --- CORE LOGIC: Save System ---
async function saveVerifiedPortalWithMetadata(portalData: any): Promise<boolean> {
    try {
        const kv = await Deno.openKv();
        const mainKey = ["VERIFIED_JOB_PORTALS"];
        const portalsResult = await kv.get<any[]>(mainKey);
        const currentPortals = portalsResult.value || [];

        if (currentPortals.some(p => p.url === portalData.final_url)) return false; // Already exists

        const portalEntry = {
            url: portalData.final_url,
            discovered_at: new Date().toISOString(),
            confidence_score: portalData.confidence_score,
            it_job_count_estimate: portalData.it_job_count_estimate || 0,
            portal_quality: portalData.portal_quality || 'unknown',
            portal_type: portalData.portal_type || 'unknown'
        };
        currentPortals.push(portalEntry);
        await kv.set(mainKey, currentPortals);

        console.log(`✅ SAVED PORTAL: ${portalData.final_url} (Type: ${portalData.portal_type}, Quality: ${portalData.portal_quality})`);
        return true;
    } catch (error) {
        console.error(`❌ Failed to save portal: ${error.message}`);
        return false;
    }
}


// --- MAIN ORCHESTRATOR ---
async function runSingularityOrchestration() {
    console.log("\n🚀 === SINGULARITY MODE ACTIVATED (LLM-NATIVE) ===");
    const kv = await Deno.openKv();

    let seedLibrary = (await kv.get<any>(["SEED_LIBRARY_V2"])).value || {};
    let analytics = (await kv.get<any>(["ANALYTICS_V2"])).value || {
        successfulPatterns: [], failedPatterns: [], consecutiveFailures: 0, totalDiscovered: 0
    };
    if (Object.keys(seedLibrary).length === 0) {
        SEED_EXPANSION_STRATEGIES.forEach(url => { seedLibrary[url] = { score: 1, last_visited: "2000-01-01T00:00:00Z" }; });
    }

    const initialPortalCount = ((await kv.get<any[]>(["VERIFIED_JOB_PORTALS"])).value || []).length;
    const shouldExplore = analytics.consecutiveFailures < 5 && Object.keys(seedLibrary).length > 0;

    if (shouldExplore) {
        console.log("📊 MODE: PARALLEL EXPLORATION (LLM-DRIVEN EXTRACTION)");
        const twentyFourHoursAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);
        const eligibleSources = Object.entries(seedLibrary)
            .filter(([, data]: [string, any]) => new Date(data.last_visited) < twentyFourHoursAgo)
            .sort(([, a]: [string, any], [, b]: [string, any]) => b.score - a.score)
            .slice(0, MAX_PARALLEL_EXPLORATIONS);

        if (eligibleSources.length > 0) {
            const explorationPromises = eligibleSources.map(async ([sourceUrl, sourceData]: [string, any]) => {
                try {
                    seedLibrary[sourceUrl].last_visited = new Date().toISOString();
                    const response = await safeFetch(sourceUrl, 15000, { headers: { 'User-Agent': BROWSER_USER_AGENT } });
                    if (!response.ok) return { portals: [], companies: [] };

                    const pageText = await response.text();
                    const extractionPrompt = `
Extract job portal URLs and company names from this content.
Focus on Indian IT, software, and data science sectors.

Content: """${pageText.substring(0, 50000)}"""

Respond with a single JSON object:
{
  "portals": ["url1", "url2"],
  "companies": ["companyName1", "companyName2"]
}`;
                    return await callGeminiWithFallback(extractionPrompt);
                } catch (error) {
                    console.warn(`⚠️ Exploration failed for ${sourceUrl}: ${error.message}`);
                    return { portals: [], companies: [] };
                }
            });

            const explorationResults = await Promise.allSettled(explorationPromises);
            const urlsToVerify = new Set<string>();
            let companyNamesToPredict = new Set<string>();

            explorationResults.forEach(res => {
                if (res.status === 'fulfilled' && res.value) {
                    (res.value.portals || []).forEach(url => url && urlsToVerify.add(url));
                    (res.value.companies || []).forEach(name => name && companyNamesToPredict.add(name));
                }
            });
            
            // Augment with LLM-predicted company URLs
            const predictedUrls = await predictCompanyCareerUrls(Array.from(companyNamesToPredict));
            predictedUrls.forEach(url => url && urlsToVerify.add(url));

            console.log(`🧠 LLM extracted ${urlsToVerify.size} potential URLs for verification...`);
            
            // Parallel Verification
            const urlArray = Array.from(urlsToVerify);
            let newPortalsFound = 0;
            for (let i = 0; i < urlArray.length; i += MAX_PARALLEL_VERIFICATIONS) {
                const batch = urlArray.slice(i, i + MAX_PARALLEL_VERIFICATIONS);
                const verificationPromises = batch.map(url => runAdvancedVerifier(url));
                const batchResults = await Promise.allSettled(verificationPromises);

                for (const res of batchResults) {
                    if (res.status === 'fulfilled' && res.value?.is_job_portal && res.value.confidence_score > PORTAL_CONFIDENCE_THRESHOLD) {
                        if (await saveVerifiedPortalWithMetadata(res.value)) newPortalsFound++;
                    }
                }
                if (i + MAX_PARALLEL_VERIFICATIONS < urlArray.length) await new Promise(resolve => setTimeout(resolve, 2000));
            }
            if (newPortalsFound > 0) analytics.consecutiveFailures = 0;
            else analytics.consecutiveFailures++;
        }
    } else {
        console.log("🔄 MODE: ADAPTIVE DISCOVERY (LLM-DRIVEN QUERY GENERATION)");
        const categories = ['IT_GENERAL', 'DATA_SCIENCE', 'STARTUP_ECOSYSTEM', 'REGIONAL_FOCUS', 'UNCONVENTIONAL'];
        const focusCategory = categories[analytics.consecutiveFailures % categories.length];
        
        console.log(`🎯 Current Focus: ${focusCategory}`);
        const searchQueries = await generateAdaptiveSearchQueries(analytics, focusCategory);
        analytics.successfulPatterns.push(...searchQueries); // Assume success for now

        const discoveryResults = await runMultiThreadedDiscovery(searchQueries);
        const newSeedUrls = new Set<string>();
        discoveryResults.forEach(res => {
            if (res.status === 'fulfilled' && res.value?.success) {
                res.value.results.forEach((item: any) => item.link && !seedLibrary[item.link] && newSeedUrls.add(item.link));
            }
        });

        console.log(`🌱 Discovered ${newSeedUrls.size} new seed URLs to investigate.`);
        newSeedUrls.forEach(url => { seedLibrary[url] = { score: 1, last_visited: "2000-01-01T00:00:00Z" }; });
        
        // --- FIX APPLIED HERE ---
        // Only increment failures if the discovery yields zero new seeds.
        // Do NOT decrement failures, as finding seeds is not the same as finding a valid portal.
        // The counter will only be reset to 0 upon a successful portal save.
        if (newSeedUrls.size === 0) {
            analytics.consecutiveFailures++;
        }
    }

    await kv.set(["SEED_LIBRARY_V2"], seedLibrary);
    await kv.set(["ANALYTICS_V2"], analytics);

    const finalPortalCount = ((await kv.get<any[]>(["VERIFIED_JOB_PORTALS"])).value || []).length;
    console.log("\n📊 === CYCLE SUMMARY ===");
    console.log(`🆕 New portals this cycle: ${finalPortalCount - initialPortalCount}`);
    console.log(`🏆 Total verified portals: ${finalPortalCount}`);
    console.log(`🌱 Seed library size: ${Object.keys(seedLibrary).length}`);
    console.log(`❌ Consecutive failures: ${analytics.consecutiveFailures}`);
}

// --- DENO DEPLOY ENTRYPOINTS ---
Deno.cron("Singularity Job Bot", "*/8 * * * *", () => {
    console.log("⏰ Singularity Bot triggered by cron.");
    runSingularityOrchestration().catch(console.error);
});

Deno.serve(async (req) => {
    const url = new URL(req.url);
    if (url.pathname === '/stats') {
        const kv = await Deno.openKv();
        const portals = (await kv.get<any[]>(["VERIFIED_JOB_PORTALS"])).value || [];
        const analytics = (await kv.get<any>(["ANALYTICS_V2"])).value || {};
        return new Response(JSON.stringify({ total_portals: portals.length, analytics, recent_portals: portals.slice(-10) }, null, 2), { headers: { 'Content-Type': 'application/json' } });
    }
    if (url.pathname === '/trigger') {
        console.log("👋 Manual Singularity Bot trigger received.");
        runSingularityOrchestration().catch(console.error);
        return new Response("Singularity Bot cycle triggered! Check logs for progress.");
    }
    return new Response("Singularity Bot Operational. Use /trigger to run or /stats to view data.");
});
