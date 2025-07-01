// =================================================================
// The "Research Strategist" Bot v10.3 (Deno Deploy - FULL Translation)
// All logic, including SERPAPI Discovery, is now correctly translated.
// =================================================================

// --- HELPER: Gemini API Call with Fallback ---
async function callGeminiWithFallback(prompt: string) {
  const primaryModel = "gemini-2.5-pro";
  const secondaryModel = "gemini-2.0-flash";
  const keysEnv = Deno.env.get("GEMINI_API_KEYS") || "";
  const keys = keysEnv.split(',').map(k => k.trim()).filter(Boolean);
  if (keys.length === 0) throw new Error("GEMINI_API_KEYS environment variable is not set.");
  const shuffledKeys = keys.sort(() => 0.5 - Math.random());

  for (const key of shuffledKeys) {
    for (const model of [primaryModel, secondaryModel]) {
      try {
        const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }] })
        });
        if (res.ok) {
          const result = await res.json();
          if (result.candidates && result.candidates.length > 0) {
            console.log(`Success with model: ${model}`);
            return result;
          }
        }
      } catch (e) {
        console.warn(`An exception occurred with model ${model}: ${e.message}. Trying next.`);
      }
    }
  }
  throw new Error("All provided API keys failed on both primary and fallback models.");
}

// --- HELPER: Save verified page immediately to Deno KV ---
async function saveVerifiedPageImmediately(newJobPageUrl: string): Promise<boolean> {
  try {
    const kv = await Deno.openKv();
    const key = ["VERIFIED_JOB_PAGES"];
    const currentPagesResult = await kv.get<string[]>(key);
    const currentPages = currentPagesResult.value || [];
    if (!currentPages.includes(newJobPageUrl)) {
      currentPages.push(newJobPageUrl);
      await kv.set(key, currentPages);
      console.log(`✅ IMMEDIATELY SAVED: ${newJobPageUrl} to KV storage`);
      return true;
    }
    return false;
  } catch (error) {
    console.error(`❌ Failed to immediately save verified page: ${error.message}`);
    return false;
  }
}

// --- HELPER: Creativity Engine (Restored from Original) ---
function generateCreativePrompt(creativityLevel: number, successfulPatterns: string[], failedPatterns: string[]) {
  const baseContext = "You are an AI market research specialist focused on discovering Indian IT, data science, and AI job opportunities.";
  let creativityInstruction = "";
  let avoidanceInstruction = failedPatterns.length > 0 ? `\n\nCRITICAL: Avoid these recently failed approaches: ${JSON.stringify(failedPatterns.slice(-15))}` : "";
  let successInstruction = successfulPatterns.length > 0 ? `\n\nINSPIRATION: These approaches worked well previously: ${JSON.stringify(successfulPatterns.slice(-10))}` : "";
  switch (creativityLevel) {
    case 1: creativityInstruction = `Generate 3 Google search queries to find Indian IT company career pages and established job portals. Focus on software development, data science, and AI roles.`; break;
    case 2: creativityInstruction = `Generate 3 Google search queries with slight variations of proven approaches. Mix established job portals with company-specific searches for Indian IT, data, and AI roles.`; break;
    case 3: creativityInstruction = `Think creatively! Generate 3 Google search queries that explore cross-domain opportunities - like fintech companies needing ML engineers, edtech startups hiring data scientists, or logistics companies building AI teams.`; break;
    case 4: creativityInstruction = `Be innovative! Generate 3 completely novel Google search queries. Think about unexplored angles: government tech initiatives, research institution collaborations, or emerging tech hubs in tier-2 Indian cities.`; break;
    case 5: creativityInstruction = `MAXIMUM CREATIVITY MODE! Generate 3 wildly different, contrarian Google search queries. Think outside conventional wisdom - maybe Indian subsidiaries of global companies, stealth mode startups, or unconventional hiring channels for tech roles.`; break;
    default: creativityInstruction = `Generate 3 precise Google search queries to find Indian IT job opportunities.`;
  }
  return `${baseContext} ${creativityInstruction}${successInstruction}${avoidanceInstruction}`;
}

// --- HELPER: Performance Analytics (Restored from Original) ---
function updatePerformanceAnalytics(analytics: any, query: string, foundNewPage: boolean) {
  if (!analytics.queryPatterns) analytics.queryPatterns = [];
  if (!analytics.successfulPatterns) analytics.successfulPatterns = [];
  if (!analytics.failedPatterns) analytics.failedPatterns = [];
  analytics.queryPatterns.push({ query: query, timestamp: new Date().toISOString(), success: foundNewPage });
  if (foundNewPage) {
    analytics.successfulPatterns.push(query);
  } else {
    analytics.failedPatterns.push(query);
  }
  if (analytics.successfulPatterns.length > 20) analytics.successfulPatterns.shift();
  if (analytics.failedPatterns.length > 30) analytics.failedPatterns.shift();
  if (analytics.queryPatterns.length > 100) analytics.queryPatterns.shift();
  return analytics;
}

const BROWSER_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36';

// --- LOGIC: The Verifier ---
async function runVerifier(companyName: string) {
    const predictorPrompt = `You are an expert system that predicts careers page URLs for Indian IT, data science, and AI companies. Company Name: "${companyName}". Consider standard patterns like /careers, /jobs, /career, /join-us, /life-at-[company]. Your response MUST be a single, valid JSON object in the format: {"predicted_url": "string"}.`;
    const predictionResult = await callGeminiWithFallback(predictorPrompt);
    const jsonStringPred = predictionResult.candidates[0].content.parts[0].text.replaceAll('```json', '').replaceAll('```', '').trim();
    const { predicted_url: careers_url } = JSON.parse(jsonStringPred);
    if (!careers_url) throw new Error("Could not predict a careers URL.");
    const siteResponse = await fetch(careers_url, { headers: { 'User-Agent': BROWSER_USER_AGENT } });
    if (!siteResponse.ok) throw new Error(`Predicted URL invalid. Status: ${siteResponse.status}`);
    const pageText = await siteResponse.text();
    const MAX_CHARS_FOR_VERIFICATION = 30000;
    const verificationPrompt = `You are a recruitment analyst specializing in Indian IT, data science, and AI roles. Analyze the provided webpage text to determine if it contains legitimate job opportunities... Look for POSITIVE and NEGATIVE indicators... Your response MUST be a single, valid JSON object in the format: { "is_careers_page": boolean, "confidence_score": float, "final_url": "${careers_url}" }. Webpage text: """${pageText.substring(0, MAX_CHARS_FOR_VERIFICATION)}"""`;
    const verificationResult = await callGeminiWithFallback(verificationPrompt);
    const jsonStringVer = verificationResult.candidates[0].content.parts[0].text.replaceAll('```json', '').replaceAll('```', '').trim();
    return JSON.parse(jsonStringVer);
}

// --- LOGIC: The Explorer ---
async function runExplorer(sourceUrl: string) {
    const sourceResponse = await fetch(sourceUrl, { headers: { 'User-Agent': BROWSER_USER_AGENT } });
    if (!sourceResponse.ok) {
        console.warn(`Failed to fetch source URL: ${sourceUrl} with status ${sourceResponse.status}`);
        return [];
    }
    const pageText = await sourceResponse.text();
    const geminiPrompt = `You are an AI business intelligence analyst specializing in Indian IT, data science, and AI companies. Extract company names from the provided article text... Your response MUST be a single, valid JSON array of objects in the format: [{ "company_name": "string" }]. If no relevant companies are found, return an empty array []. Article text: """${pageText.substring(0, 50000)}"""`;
    const geminiResult = await callGeminiWithFallback(geminiPrompt);
    const jsonString = geminiResult.candidates[0].content.parts[0].text.replace(/```json/g, '').replace(/```/g, '').trim();
    return JSON.parse(jsonString);
}

// --- LOGIC: The Main Orchestrator ---
async function runOrchestration() {
  console.log("\n--- Hunter Cycle Starting (Deno Version - Full Logic) ---");
  const kv = await Deno.openKv();

  let seedLibrary = (await kv.get<Record<string, {score: number, last_visited: string}>>(["BOT_MEMORY", "SEED_LIBRARY"])).value || {"https://inc42.com/features/":{"score":1,"last_visited":"2000-01-01T00:00:00Z"}};
  let processedCompanies = (await kv.get<Record<string, string>>(["BOT_MEMORY", "PROCESSED_COMPANIES"])).value || {};
  let verifiedJobPages = (await kv.get<string[]>(["VERIFIED_JOB_PAGES"])).value || [];
  let promptHistory = (await kv.get<string[]>(["PROMPT_HISTORY"])).value || [];
  let performanceAnalytics = (await kv.get<any>(["PERFORMANCE_ANALYTICS"])).value || { queryPatterns: [], successfulPatterns: [], failedPatterns: [], consecutiveFailures: 0 };
  
  const initialVerifiedCount = verifiedJobPages.length;

  const twentyThreeHoursAgo = new Date(Date.now() - 23 * 60 * 60 * 1000);
  const eligibleSources = Object.entries(seedLibrary).filter(([_, data]) => new Date(data.last_visited) < twentyThreeHoursAgo);

  if (eligibleSources.length > 0) {
    console.log("Mode: EXPLORING (with PARALLEL verification and INSTANT saves).");
    eligibleSources.sort(([, a], [, b]) => b.score - a.score);
    const [sourceToExplore, sourceData] = eligibleSources[0];
    console.log(`Target: ${sourceToExplore} (Score: ${sourceData.score})`);
    seedLibrary[sourceToExplore].last_visited = new Date().toISOString();

    const companyLeads = await runExplorer(sourceToExplore);
    const allNewLeads = companyLeads.filter((c: any) => c.company_name && !processedCompanies[c.company_name.toLowerCase()]);
    console.log(`Explorer found ${allNewLeads.length} new leads.`);

    const maxCompaniesToProcess = 12;
    const leadsToProcess = allNewLeads.slice(0, maxCompaniesToProcess);
    
    if (leadsToProcess.length > 0) {
      console.log(`🚀 Starting parallel verification & save for ${leadsToProcess.length} companies...`);
      const processingPromises = leadsToProcess.map((company: any) => {
        processedCompanies[company.company_name.toLowerCase()] = new Date().toISOString();
        return runVerifier(company.company_name)
          .then(async (verifierData) => {
              if (verifierData.is_careers_page && verifierData.confidence_score > 0.8) {
                  const saved = await saveVerifiedPageImmediately(verifierData.final_url);
                  return { newPageFound: saved, url: verifierData.final_url };
              }
              return { newPageFound: false };
          })
          .catch(err => {
              console.warn(`Verification failed for ${company.company_name}: ${err.message}`);
              return { newPageFound: false };
          });
      });
      await Promise.allSettled(processingPromises);
    }
  } else {
    console.log("Mode: DISCOVERY.");
    const creativityLevel = Math.min(5, Math.floor(performanceAnalytics.consecutiveFailures / 5) + 1);
    console.log(`Creativity Level: ${creativityLevel}/5`);

    const basePrompt = generateCreativePrompt(creativityLevel, performanceAnalytics.successfulPatterns, performanceAnalytics.failedPatterns);
    const discoveryPrompt = `Your response MUST be a single, valid JSON array of strings. Here is your task:\n${basePrompt}`;
    const geminiData = await callGeminiWithFallback(discoveryPrompt);
    const jsonString = geminiData.candidates[0].content.parts[0].text.replaceAll('```json', '').replaceAll('```', '').trim();
    const searchQueries = JSON.parse(jsonString);

    const query = searchQueries[Math.floor(Math.random() * searchQueries.length)];
    promptHistory.push(query); if (promptHistory.length > 20) promptHistory.shift();
    console.log(`Googling with Creativity Level ${creativityLevel}: "${query}"`);

    const serpApiKeys = (Deno.env.get("SERPAPI_KEYS") || "").split(',').map(k => k.trim()).filter(Boolean);
    if(serpApiKeys.length === 0) throw new Error("SERPAPI_KEYS environment variable is not set.");
    const shuffledSerpKeys = serpApiKeys.sort(() => 0.5 - Math.random());
    
    let searchResults = null;
    for (const apiKey of shuffledSerpKeys) {
        const searchRes = await fetch(`https://serpapi.com/search.json?q=${encodeURIComponent(query)}&api_key=${apiKey}&google_domain=google.co.in&gl=in`);
        if (searchRes.ok) {
            const data = await searchRes.json();
            if (!data.error) {
                searchResults = data;
                break;
            }
        }
    }

    if (searchResults && searchResults.organic_results && searchResults.organic_results.length > 0) {
        const newSeedUrl = searchResults.organic_results[0].link;
        if (!seedLibrary[newSeedUrl]) {
            console.log(`Found a promising new source: ${newSeedUrl}.`);
            seedLibrary[newSeedUrl] = { score: 1, last_visited: "2000-01-01T00:00:00Z" };
            performanceAnalytics = updatePerformanceAnalytics(performanceAnalytics, query, true);
        } else {
            performanceAnalytics.consecutiveFailures++;
        }
    } else { 
        console.log("DISCOVERY search yielded no organic results.");
        performanceAnalytics = updatePerformanceAnalytics(performanceAnalytics, query, false);
        performanceAnalytics.consecutiveFailures++;
    }
  }

  // Save ALL state back to Deno KV
  await kv.set(["BOT_MEMORY", "SEED_LIBRARY"], seedLibrary);
  await kv.set(["BOT_MEMORY", "PROCESSED_COMPANIES"], processedCompanies);
  await kv.set(["PROMPT_HISTORY"], promptHistory);
  await kv.set(["PERFORMANCE_ANALYTICS"], performanceAnalytics);

  // Final status log
  const currentVerifiedPages = (await kv.get<string[]>(["VERIFIED_JOB_PAGES"])).value || [];
  const newDiscoveries = currentVerifiedPages.length - initialVerifiedCount;
  if (newDiscoveries > 0) {
      console.log(`🎉 MISSION ACCOMPLISHED! Found ${newDiscoveries} new page(s) this cycle.`);
      performanceAnalytics.consecutiveFailures = 0;
      await kv.set(["PERFORMANCE_ANALYTICS"], performanceAnalytics); // Save the reset counter
  } else {
      console.log("Cycle complete. No new pages found.");
  }
}

// --- DENO DEPLOY ENTRYPOINTS ---
Deno.cron(
  "Job Bot Orchestrator",
  "*/10 * * * *",
  () => {
    console.log("⏰ Cron job triggered by schedule.");
    runOrchestration();
  }
);

Deno.serve(async (req) => {
  console.log("👋 Manual HTTP trigger received.");
  runOrchestration();
  return new Response("Orchestration cycle triggered manually! Check logs for progress.");
});
