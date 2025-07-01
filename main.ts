// =================================================================
// The "Research Strategist" Bot v10.1 (Deno Deploy - Exact Translation)
// All-in-one, resilient, and running on Deno.
// This version is a direct translation of the original Cloudflare logic.
// =================================================================

// --- HELPER: Gemini API Call with Fallback (Exact Translation) ---
async function callGeminiWithFallback(prompt: string) {
  const primaryModel = "gemini-2.5-pro";
  const secondaryModel = "gemini-2.0-flash";

  // Deno's way to get environment variables
  const keysEnv = Deno.env.get("GEMINI_API_KEYS") || "";
  const keys = keysEnv.split(',').map(k => k.trim()).filter(Boolean);

  if (keys.length === 0) {
    throw new Error("GEMINI_API_KEYS environment variable is not set or contains no keys.");
  }

  // Shuffle keys to distribute load, exactly like the original logic
  const shuffledKeys = keys.sort(() => 0.5 - Math.random());

  for (const key of shuffledKeys) {
    // --- Attempt 1: Primary Model ---
    try {
      const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${primaryModel}:generateContent?key=${key}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }] })
      });
      if (res.ok) {
        const result = await res.json();
        if (result.candidates && result.candidates.length > 0) {
          console.log(`Success with primary model: ${primaryModel}`);
          return result;
        }
      }
      console.warn(`Primary model failed with status: ${res.status}. Trying fallback.`);
    } catch (e) {
      console.warn(`An exception occurred with primary model: ${e.message}. Trying fallback.`);
    }

    // --- Attempt 2: Fallback Model (with the same key) ---
    try {
      const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${secondaryModel}:generateContent?key=${key}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }] })
      });
      if (res.ok) {
        const result = await res.json();
        if (result.candidates && result.candidates.length > 0) {
          console.log(`Success with fallback model: ${secondaryModel}`);
          return result;
        }
      }
      console.warn(`Fallback model also failed for this key. Trying next key.`);
    } catch (e) {
      console.warn(`An exception occurred with fallback model: ${e.message}. Trying next key.`);
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

// --- LOGIC: The Verifier ---
async function runVerifier(companyName: string) {
    const predictorPrompt = `You are an expert system that predicts careers page URLs for Indian IT, data science, and AI companies. Company Name: "${companyName}". Consider standard patterns like /careers, /jobs, etc. Your response MUST be a single, valid JSON object in the format: {"predicted_url": "string"}.`;
    
    const predictionResult = await callGeminiWithFallback(predictorPrompt);
    const jsonStringPred = predictionResult.candidates[0].content.parts[0].text.replaceAll('```json', '').replaceAll('```', '').trim();
    const { predicted_url: careers_url } = JSON.parse(jsonStringPred);

    if (!careers_url) throw new Error("Could not predict a careers URL.");

    const siteResponse = await fetch(careers_url, { headers: { 'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)' } });
    if (!siteResponse.ok) throw new Error(`Predicted URL invalid. Status: ${siteResponse.status}`);
    
    const pageText = await siteResponse.text();
    const MAX_CHARS_FOR_VERIFICATION = 30000;
    const verificationPrompt = `You are a recruitment analyst. Analyze the provided webpage text to determine if it contains legitimate job opportunities in IT, data science, or AI. Your response MUST be a single, valid JSON object in the format: { "is_careers_page": boolean, "confidence_score": float, "final_url": "${careers_url}" }. Webpage text: """${pageText.substring(0, MAX_CHARS_FOR_VERIFICATION)}"""`;

    const verificationResult = await callGeminiWithFallback(verificationPrompt);
    const jsonStringVer = verificationResult.candidates[0].content.parts[0].text.replaceAll('```json', '').replaceAll('```', '').trim();
    return JSON.parse(jsonStringVer);
}

// --- LOGIC: The Explorer ---
async function runExplorer(sourceUrl: string) {
    const sourceResponse = await fetch(sourceUrl, { headers: { 'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)' } });
    if (!sourceResponse.ok) {
        console.warn(`Failed to fetch source URL: ${sourceUrl} with status ${sourceResponse.status}`);
        return [];
    }
    
    const pageText = await sourceResponse.text();
    const geminiPrompt = `You are an AI business intelligence analyst. Extract company names from the provided article text. Focus on IT, data science, and AI companies. Your response MUST be a single, valid JSON array of objects in the format: [{ "company_name": "string" }]. If no relevant companies are found, return an empty array []. Article text: """${pageText.substring(0, 50000)}"""`;

    const geminiResult = await callGeminiWithFallback(geminiPrompt);
    const jsonString = geminiResult.candidates[0].content.parts[0].text.replace(/```json/g, '').replace(/```/g, '').trim();
    return JSON.parse(jsonString);
}


// --- LOGIC: The Main Orchestrator ---
async function runOrchestration() {
  console.log("\n--- Hunter Cycle Starting (Deno Version) ---");
  const kv = await Deno.openKv();

  const seedLibrary = (await kv.get<Record<string, {score: number, last_visited: string}>>(["BOT_MEMORY", "SEED_LIBRARY"])).value || {"https://inc42.com/features/":{"score":1,"last_visited":"2000-01-01T00:00:00Z"}};
  let processedCompanies = (await kv.get<Record<string, string>>(["BOT_MEMORY", "PROCESSED_COMPANIES"])).value || {};
  let verifiedJobPages = (await kv.get<string[]>(["VERIFIED_JOB_PAGES"])).value || [];
  
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

      const processingResults = await Promise.allSettled(processingPromises);
      const newDiscoveriesInBatch = processingResults.filter(r => r.status === 'fulfilled' && r.value.newPageFound).length;
      console.log(`✅ All parallel tasks complete. ${newDiscoveriesInBatch} new page(s) were found and saved.`);
    }

    const newDiscoveries = (await kv.get<string[]>(["VERIFIED_JOB_PAGES"])).value.length - initialVerifiedCount;
    if (newDiscoveries > 0) {
        seedLibrary[sourceToExplore].score += newDiscoveries;
        console.log(`Source rewarded. New score: ${seedLibrary[sourceToExplore].score}`);
    } else {
        seedLibrary[sourceToExplore].score > 0 ? seedLibrary[sourceToExplore].score = 0 : seedLibrary[sourceToExplore].score--;
        console.log(`Source penalized. New score: ${seedLibrary[sourceToExplore].score}`);
    }
    if (seedLibrary[sourceToExplore].score < -5) {
        console.log(`Pruning low-value source: ${sourceToExplore}`);
        delete seedLibrary[sourceToExplore];
    }
  } else {
    console.log("Mode: DISCOVERY. No eligible sources to explore. Will try again next cycle.");
  }

  await kv.set(["BOT_MEMORY", "SEED_LIBRARY"], seedLibrary);
  await kv.set(["BOT_MEMORY", "PROCESSED_COMPANIES"], processedCompanies);

  const finalVerifiedCount = (await kv.get<string[]>(["VERIFIED_JOB_PAGES"])).value.length;
  if(finalVerifiedCount > initialVerifiedCount) {
      console.log(`🎉 MISSION ACCOMPLISHED! Found ${finalVerifiedCount - initialVerifiedCount} new page(s) this cycle.`);
  } else {
      console.log("Cycle complete. No new pages found.");
  }
}

// --- DENO DEPLOY ENTRYPOINTS ---

// 1. The 24/7 Scheduler
Deno.cron(
  "Job Bot Orchestrator",
  "*/10 * * * *",
  () => {
    console.log("⏰ Cron job triggered by schedule.");
    runOrchestration();
  }
);

// 2. The Manual Trigger (for testing)
Deno.serve(async (req) => {
  console.log("👋 Manual HTTP trigger received.");
  runOrchestration();
  return new Response("Orchestration cycle triggered manually! Check logs for progress.");
});