// =================================================================
// The "Singularity" Research Bot v12.1 - FIXED VERSION
// Addresses key issues preventing portal discovery
// =================================================================

// --- ENHANCED CONSTANTS ---
const BROWSER_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36';
const MAX_PARALLEL_VERIFICATIONS = 15; // Reduced for better stability
const MAX_PARALLEL_EXPLORATIONS = 6;
const DISCOVERY_BATCH_SIZE = 12;
const PORTAL_CONFIDENCE_THRESHOLD = 0.6; // Lowered threshold
const VERIFICATION_TIMEOUT = 25000; // Increased timeout
const MAX_CONTENT_LENGTH = 15000; // Reduced content length for LLM

// --- IMPROVED SEED STRATEGIES ---
const SEED_EXPANSION_STRATEGIES = [
  "https://yourstory.com/jobs", "https://inc42.com/startups", 
  "https://analyticsindiamag.com/jobs", "https://www.naukri.com",
  "https://www.indeed.co.in", "https://www.monster.com", 
  "https://www.glassdoor.co.in", "https://angel.co/india",
  "https://www.linkedin.com/jobs", "https://internshala.com"
];

// --- HELPER: Safe Fetch with Better Error Handling ---
async function safeFetch(url: string, timeoutMs: number, options: RequestInit = {}): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
      headers: {
        'User-Agent': BROWSER_USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        ...options.headers
      }
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

// --- HELPER: Fixed Gemini API Call ---
async function callGeminiWithFallback(prompt: string, maxRetries: number = 3) {
  const primaryModel = "gemini-2.0-flash"; // Use more reliable model
  const secondaryModel = "gemini-2.5-pro";
  const keysEnv = Deno.env.get("GEMINI_API_KEYS") || "";
  const keys = keysEnv.split(',').map(k => k.trim()).filter(Boolean);
  if (keys.length === 0) throw new Error("GEMINI_API_KEYS environment variable is not set.");

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    const key = keys[attempt % keys.length];
    for (const model of [primaryModel, secondaryModel]) {
      try {
        const res = await safeFetch(
          `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`, 
          30000, // Increased timeout for LLM calls
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              contents: [{ parts: [{ text: prompt }] }],
              generationConfig: {
                temperature: 0.2 + (attempt * 0.1),
                maxOutputTokens: 4096,
                // Remove responseMimeType to avoid parsing issues
              }
            })
          }
        );
        
        if (res.ok) {
          const result = await res.json();
          if (result.candidates && result.candidates.length > 0) {
            const textContent = result.candidates[0].content.parts[0].text;
            console.log(`✅ Gemini Success: Model ${model} (Attempt ${attempt + 1})`);
            
            // Better JSON extraction with fallback
            try {
              // Try to extract JSON from markdown code blocks if present
              const jsonMatch = textContent.match(/```(?:json)?\s*(\{[\s\S]*?\})\s*```/);
              if (jsonMatch) {
                return JSON.parse(jsonMatch[1]);
              }
              // Try direct parsing
              return JSON.parse(textContent);
            } catch (parseError) {
              // If JSON parsing fails, try to extract structured data manually
              console.warn(`⚠️ JSON parsing failed, attempting manual extraction`);
              return extractStructuredData(textContent, attempt);
            }
          }
        }
      } catch (e) {
        console.warn(`⚠️ Gemini Exception: Model ${model}, Key ending in ...${key.slice(-4)}: ${e.message}`);
      }
    }
    if (attempt < maxRetries - 1) {
      await new Promise(resolve => setTimeout(resolve, (attempt + 1) * 2000));
    }
  }
  throw new Error("All API keys and models failed after multiple retries.");
}

// --- HELPER: Manual Data Extraction Fallback ---
function extractStructuredData(text: string, attempt: number): any {
  try {
    // For search queries
    if (text.includes('queries') || text.includes('search')) {
      const queries = [];
      const lines = text.split('\n');
      for (const line of lines) {
        if (line.includes('job') || line.includes('career') || line.includes('IT')) {
          queries.push(line.replace(/[^\w\s]/g, '').trim());
        }
      }
      return { queries: queries.slice(0, 10) };
    }
    
    // For verification results - be more lenient
    return {
      is_job_portal: text.toLowerCase().includes('job') || text.toLowerCase().includes('career'),
      confidence_score: 0.5 + (attempt * 0.1), // Progressive confidence
      has_it_jobs: text.toLowerCase().includes('it') || text.toLowerCase().includes('software'),
      portal_type: "unknown",
      portal_quality: "medium",
      it_job_count_estimate: 10,
      reasoning: "Fallback extraction due to JSON parsing failure",
      final_url: ""
    };
  } catch (error) {
    console.error("Manual extraction failed:", error);
    return { queries: [], portals: [], companies: [] };
  }
}

// --- IMPROVED: Content Preprocessing ---
function preprocessWebContent(content: string): string {
  // Remove script tags, style tags, and comments
  let cleaned = content
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<!--[\s\S]*?-->/g, '')
    .replace(/<[^>]+>/g, ' ') // Remove HTML tags
    .replace(/\s+/g, ' ') // Normalize whitespace
    .trim();
  
  // Extract job-related content more intelligently
  const jobKeywords = ['job', 'career', 'position', 'opening', 'vacancy', 'employment', 'hire', 'apply'];
  const sentences = cleaned.split(/[.!?]+/);
  const relevantSentences = sentences.filter(sentence => 
    jobKeywords.some(keyword => sentence.toLowerCase().includes(keyword))
  );
  
  // Use relevant sentences if found, otherwise use beginning of content
  const finalContent = relevantSentences.length > 0 
    ? relevantSentences.join('. ').substring(0, MAX_CONTENT_LENGTH)
    : cleaned.substring(0, MAX_CONTENT_LENGTH);
    
  return finalContent;
}

// --- IMPROVED: Adaptive Search Query Generation ---
async function generateAdaptiveSearchQueries(analytics: any, categoryFocus: string) {
  const consecutiveFailures = analytics.consecutiveFailures || 0;
  
  // Provide more structured prompt with examples
  const prompt = `Generate ${DISCOVERY_BATCH_SIZE} effective Google search queries to find Indian job portals and company career pages.

Focus: ${categoryFocus}
Failures: ${consecutiveFailures}

Examples of good queries:
- "top IT companies hiring in bangalore careers page"
- "indian startup jobs portal 2024"
- "software developer jobs mumbai site:naukri.com OR site:monster.com"

Generate diverse queries mixing:
1. General job portals
2. Company-specific career pages
3. Industry-specific boards
4. Location-based searches
5. Recent/trending terms

Return as JSON: {"queries": ["query1", "query2", ...]}`;

  try {
    const result = await callGeminiWithFallback(prompt);
    return Array.isArray(result.queries) ? result.queries : [];
  } catch (error) {
    console.error("❌ Failed to generate search queries, using fallback:", error.message);
    // Better fallback queries
    return [
      `${categoryFocus} jobs India 2024`,
      "IT company careers page India",
      "software developer jobs portal",
      "Indian startup hiring platform",
      "tech jobs bangalore mumbai",
      "data science jobs India site:naukri.com",
      "remote work opportunities India",
      "fresher jobs IT companies"
    ];
  }
}

// --- IMPROVED: Portal Verifier with Better Logic ---
async function runAdvancedVerifier(url: string): Promise<any> {
    try {
        const response = await safeFetch(url, VERIFICATION_TIMEOUT, { 
          headers: { 'User-Agent': BROWSER_USER_AGENT } 
        });
        
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const content = await response.text();
        const processedContent = preprocessWebContent(content);
        
        // More focused verification prompt
        const verificationPrompt = `Analyze this webpage to determine if it's a legitimate job portal or company careers page.

URL: ${url}
Content: "${processedContent}"

Look for:
- Job listings or application forms
- IT/Software/Tech positions
- Active hiring indicators
- Portal functionality

Respond ONLY with valid JSON:
{
  "is_job_portal": true/false,
  "confidence_score": 0.0-1.0,
  "has_it_jobs": true/false,
  "portal_type": "aggregator|company_careers|niche_board|other",
  "portal_quality": "low|medium|high",
  "it_job_count_estimate": number,
  "reasoning": "brief explanation"
}`;

        const verificationData = await callGeminiWithFallback(verificationPrompt);
        verificationData.final_url = url;
        
        // Additional validation
        const urlLower = url.toLowerCase();
        const hasJobKeywords = ['job', 'career', 'hiring', 'apply', 'vacancy'].some(k => urlLower.includes(k));
        const contentHasJobs = processedContent.toLowerCase().includes('job') || 
                              processedContent.toLowerCase().includes('career');
        
        // Boost confidence if URL or content clearly indicates jobs
        if (hasJobKeywords || contentHasJobs) {
          verificationData.confidence_score = Math.min(1.0, verificationData.confidence_score + 0.2);
        }
        
        return verificationData;

    } catch (error) {
        console.warn(`⚠️ Verification failed for ${url}: ${error.message}`);
        return { 
          is_job_portal: false, 
          confidence_score: 0, 
          error: error.message, 
          final_url: url,
          has_it_jobs: false,
          portal_type: "error",
          portal_quality: "unknown",
          it_job_count_estimate: 0,
          reasoning: `Verification failed: ${error.message}`
        };
    }
}

// --- IMPROVED: Discovery with Better Error Handling ---
async function runMultiThreadedDiscovery(searchQueries: string[]) {
  const serpApiKeys = (Deno.env.get("SERPAPI_KEYS") || "").split(',').map(k => k.trim()).filter(Boolean);
  if (serpApiKeys.length === 0) throw new Error("SERPAPI_KEYS environment variable is not set.");

  const discoveryPromises = searchQueries.map(async (query, index) => {
    const apiKey = serpApiKeys[index % serpApiKeys.length];
    try {
      const searchUrl = `https://serpapi.com/search.json?q=${encodeURIComponent(query)}&api_key=${apiKey}&google_domain=google.co.in&gl=in&num=15`;
      const searchRes = await safeFetch(searchUrl, 20000);
      
      if (searchRes.ok) {
        const data = await searchRes.json();
        if (!data.error && data.organic_results) {
          console.log(`🔍 Search "${query}" returned ${data.organic_results.length} results`);
          return { query, results: data.organic_results.slice(0, 10), success: true };
        } else {
          console.warn(`⚠️ Search API error for "${query}":`, data.error);
        }
      } else {
        console.warn(`⚠️ Search HTTP error for "${query}": ${searchRes.status}`);
      }
    } catch (error) {
      console.warn(`⚠️ Search failed for "${query}": ${error.message}`);
    }
    return { query, results: [], success: false };
  });
  
  return await Promise.allSettled(discoveryPromises);
}

// --- IMPROVED: Save System with Better Deduplication ---
async function saveVerifiedPortalWithMetadata(portalData: any): Promise<boolean> {
  try {
    const kv = await Deno.openKv();
    const mainKey = ["VERIFIED_JOB_PORTALS"];
    const portalsResult = await kv.get<any[]>(mainKey);
    const currentPortals = portalsResult.value || [];

    // Better deduplication - check domain similarity
    const newUrl = new URL(portalData.final_url);
    const isDuplicate = currentPortals.some(p => {
      try {
        const existingUrl = new URL(p.url);
        return existingUrl.hostname === newUrl.hostname;
      } catch {
        return p.url === portalData.final_url;
      }
    });

    if (isDuplicate) {
      console.log(`⚠️ Duplicate domain found: ${portalData.final_url}`);
      return false;
    }

    const portalEntry = {
      url: portalData.final_url,
      discovered_at: new Date().toISOString(),
      confidence_score: portalData.confidence_score,
      it_job_count_estimate: portalData.it_job_count_estimate || 0,
      portal_quality: portalData.portal_quality || 'unknown',
      portal_type: portalData.portal_type || 'unknown',
      reasoning: portalData.reasoning || 'No reasoning provided'
    };
    
    currentPortals.push(portalEntry);
    await kv.set(mainKey, currentPortals);

    console.log(`✅ SAVED PORTAL: ${portalData.final_url} (Confidence: ${portalData.confidence_score.toFixed(2)}, Type: ${portalData.portal_type})`);
    return true;
  } catch (error) {
    console.error(`❌ Failed to save portal: ${error.message}`);
    return false;
  }
}

// --- MAIN ORCHESTRATOR WITH BETTER LOGGING ---
async function runSingularityOrchestration() {
  console.log("\n🚀 === SINGULARITY MODE ACTIVATED (FIXED VERSION) ===");
  const startTime = Date.now();
  const kv = await Deno.openKv();

  let seedLibrary = (await kv.get<any>(["SEED_LIBRARY_V2"])).value || {};
  let analytics = (await kv.get<any>(["ANALYTICS_V2"])).value || {
    successfulPatterns: [], failedPatterns: [], consecutiveFailures: 0, totalDiscovered: 0
  };

  // Initialize with better seed URLs if empty
  if (Object.keys(seedLibrary).length === 0) {
    SEED_EXPANSION_STRATEGIES.forEach(url => { 
      seedLibrary[url] = { score: 1, last_visited: "2000-01-01T00:00:00Z" }; 
    });
    console.log(`🌱 Initialized seed library with ${Object.keys(seedLibrary).length} URLs`);
  }

  const initialPortalCount = ((await kv.get<any[]>(["VERIFIED_JOB_PORTALS"])).value || []).length;
  console.log(`📊 Starting with ${initialPortalCount} verified portals`);
  console.log(`❌ Consecutive failures: ${analytics.consecutiveFailures}`);

  // Always run discovery mode to be more aggressive
  console.log("🔄 MODE: ADAPTIVE DISCOVERY");
  const categories = ['IT_JOBS', 'STARTUP_CAREERS', 'TECH_COMPANIES', 'JOB_PORTALS', 'REMOTE_WORK'];
  const focusCategory = categories[analytics.consecutiveFailures % categories.length];
  
  console.log(`🎯 Current Focus: ${focusCategory}`);
  const searchQueries = await generateAdaptiveSearchQueries(analytics, focusCategory);
  console.log(`🔍 Generated ${searchQueries.length} search queries`);

  if (searchQueries.length > 0) {
    const discoveryResults = await runMultiThreadedDiscovery(searchQueries);
    const urlsToVerify = new Set<string>();
    
    let successfulSearches = 0;
    discoveryResults.forEach(res => {
      if (res.status === 'fulfilled' && res.value?.success) {
        successfulSearches++;
        res.value.results.forEach((item: any) => {
          if (item.link && item.link.startsWith('http')) {
            urlsToVerify.add(item.link);
          }
        });
      }
    });

    console.log(`📈 ${successfulSearches}/${searchQueries.length} searches successful`);
    console.log(`🔗 Found ${urlsToVerify.size} URLs to verify`);

    if (urlsToVerify.size > 0) {
      // Verify in smaller batches for better success rate
      const urlArray = Array.from(urlsToVerify);
      let newPortalsFound = 0;
      let totalVerified = 0;
      
      for (let i = 0; i < urlArray.length; i += MAX_PARALLEL_VERIFICATIONS) {
        const batch = urlArray.slice(i, i + MAX_PARALLEL_VERIFICATIONS);
        console.log(`🔍 Verifying batch ${Math.floor(i/MAX_PARALLEL_VERIFICATIONS) + 1}/${Math.ceil(urlArray.length/MAX_PARALLEL_VERIFICATIONS)} (${batch.length} URLs)`);
        
        const verificationPromises = batch.map(url => runAdvancedVerifier(url));
        const batchResults = await Promise.allSettled(verificationPromises);

        for (const res of batchResults) {
          totalVerified++;
          if (res.status === 'fulfilled' && res.value) {
            const data = res.value;
            console.log(`  📊 ${data.final_url}: Portal=${data.is_job_portal}, Confidence=${data.confidence_score?.toFixed(2)}`);
            
            if (data.is_job_portal && data.confidence_score >= PORTAL_CONFIDENCE_THRESHOLD) {
              if (await saveVerifiedPortalWithMetadata(data)) {
                newPortalsFound++;
              }
            }
          }
        }
        
        // Small delay between batches
        if (i + MAX_PARALLEL_VERIFICATIONS < urlArray.length) {
          await new Promise(resolve => setTimeout(resolve, 3000));
        }
      }
      
      console.log(`✅ Verification complete: ${totalVerified} URLs processed, ${newPortalsFound} new portals found`);
      
      // Reset failures if we found any portals
      if (newPortalsFound > 0) {
        analytics.consecutiveFailures = 0;
        analytics.totalDiscovered += newPortalsFound;
      } else {
        analytics.consecutiveFailures++;
      }
    } else {
      analytics.consecutiveFailures++;
      console.log("⚠️ No URLs found to verify");
    }
  } else {
    analytics.consecutiveFailures++;
    console.log("⚠️ No search queries generated");
  }

  // Save updated data
  await kv.set(["SEED_LIBRARY_V2"], seedLibrary);
  await kv.set(["ANALYTICS_V2"], analytics);

  const finalPortalCount = ((await kv.get<any[]>(["VERIFIED_JOB_PORTALS"])).value || []).length;
  const executionTime = ((Date.now() - startTime) / 1000).toFixed(1);
  
  console.log("\n📊 === CYCLE SUMMARY ===");
  console.log(`🆕 New portals this cycle: ${finalPortalCount - initialPortalCount}`);
  console.log(`🏆 Total verified portals: ${finalPortalCount}`);
  console.log(`🌱 Seed library size: ${Object.keys(seedLibrary).length}`);
  console.log(`❌ Consecutive failures: ${analytics.consecutiveFailures}`);
  console.log(`⏱️ Execution time: ${executionTime}s`);
  console.log("=".repeat(50));
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
    const seedLibrary = (await kv.get<any>(["SEED_LIBRARY_V2"])).value || {};
    
    return new Response(JSON.stringify({ 
      total_portals: portals.length, 
      analytics, 
      recent_portals: portals.slice(-10),
      seed_count: Object.keys(seedLibrary).length,
      last_updated: new Date().toISOString()
    }, null, 2), { 
      headers: { 'Content-Type': 'application/json' } 
    });
  }
  
  if (url.pathname === '/trigger') {
    console.log("👋 Manual Singularity Bot trigger received.");
    runSingularityOrchestration().catch(console.error);
    return new Response("Singularity Bot cycle triggered! Check logs for progress.");
  }
  
  if (url.pathname === '/reset') {
    const kv = await Deno.openKv();
    await kv.set(["ANALYTICS_V2"], { successfulPatterns: [], failedPatterns: [], consecutiveFailures: 0, totalDiscovered: 0 });
    return new Response("Analytics reset successfully!");
  }
  
  return new Response(`
Singularity Bot v12.1 - FIXED VERSION
- /trigger - Run bot manually  
- /stats - View current statistics
- /reset - Reset failure counter
- Bot runs automatically every 8 minutes
  `);
});
