// =================================================================
// The "Volume Maximizer" Research Bot v11.0 - Enhanced for Scale
// Multi-threaded, intelligent portal discovery with self-evolution
// =================================================================

// --- ENHANCED CONSTANTS ---
const BROWSER_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36';
const MAX_PARALLEL_VERIFICATIONS = 25; // Increased parallelism
const MAX_PARALLEL_EXPLORATIONS = 8;
const DISCOVERY_BATCH_SIZE = 15; // Process more queries per cycle
const PORTAL_CONFIDENCE_THRESHOLD = 0.75; // Slightly lower for more inclusivity

// --- PORTAL CATEGORIES FOR INTELLIGENT TARGETING ---
const PORTAL_CATEGORIES = {
  MAJOR_PORTALS: ['naukri.com', 'monster.com', 'shine.com', 'timesjobs.com', 'indeed.co.in'],
  TECH_SPECIFIC: ['angellist.co', 'hasjob.co', 'cutshort.io', 'instahyre.com', 'hirist.com'],
  STARTUP_FOCUSED: ['wellfound.com', 'startupjobs.asia', 'founder.org'],
  COMPANY_DIRECT: ['careers pages', 'job portals', 'hiring pages'],
  GOVERNMENT: ['sarkari-naukri.info', 'freejobalert.com', 'employmentnews.gov.in'],
  FREELANCE: ['upwork.com', 'freelancer.in', 'truelancer.com'],
  REGIONAL: ['regional job portals', 'local job sites', 'city-specific hiring']
};

// --- INTELLIGENT SEED EXPANSION SYSTEM ---
const SEED_EXPANSION_STRATEGIES = [
  // Tech news sites that often feature job listings
  "https://yourstory.com/jobs",
  "https://inc42.com/startups",
  "https://entrackr.com/category/startups",
  "https://techcircle.vccircle.com/jobs",
  "https://factordaily.com",
  
  // Industry-specific platforms
  "https://analyticsindiamag.com/jobs",
  "https://www.machinehack.com/jobs",
  "https://www.kaggle.com/jobs",
  "https://datascience.com/jobs",
  
  // Government and institutional sources
  "https://www.ncs.gov.in",
  "https://meghalayajob.com",
  "https://recruitmenttoday.com",
  
  // Regional tech hubs
  "https://bangalorejobs.com",
  "https://puneitjobs.com",
  "https://hyderbabadjobs.com",
  "https://chennaijobs.com"
];

// --- HELPER: Enhanced Gemini API with Retry Logic ---
async function callGeminiWithFallback(prompt: string, maxRetries: number = 3) {
  const primaryModel = "gemini-2.5-pro";
  const secondaryModel = "gemini-2.0-flash";
  const keysEnv = Deno.env.get("GEMINI_API_KEYS") || "";
  const keys = keysEnv.split(',').map(k => k.trim()).filter(Boolean);
  
  if (keys.length === 0) throw new Error("GEMINI_API_KEYS environment variable is not set.");
  
  const shuffledKeys = keys.sort(() => 0.5 - Math.random());

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    for (const key of shuffledKeys) {
      for (const model of [primaryModel, secondaryModel]) {
        try {
          const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
              contents: [{ parts: [{ text: prompt }] }],
              generationConfig: {
                temperature: 0.1 + (attempt * 0.2), // Increase creativity on retries
                maxOutputTokens: 4096
              }
            })
          });
          
          if (res.ok) {
            const result = await res.json();
            if (result.candidates && result.candidates.length > 0) {
              console.log(`✅ Success with model: ${model} (attempt ${attempt + 1})`);
              return result;
            }
          }
        } catch (e) {
          console.warn(`⚠️ Exception with model ${model}: ${e.message}`);
        }
      }
    }
    
    if (attempt < maxRetries - 1) {
      console.log(`🔄 Retrying in ${(attempt + 1) * 1000}ms...`);
      await new Promise(resolve => setTimeout(resolve, (attempt + 1) * 1000));
    }
  }
  
  throw new Error("All API keys and models failed after multiple retries.");
}

// --- ENHANCED PATTERN LEARNING SYSTEM ---
function generateAdaptiveSearchQueries(analytics: any, categoryFocus: string = 'IT_GENERAL') {
  const successPatterns = analytics.successfulPatterns || [];
  const failedPatterns = analytics.failedPatterns || [];
  const consecutiveFailures = analytics.consecutiveFailures || 0;
  
  // Dynamic creativity based on performance
  const creativityBoost = Math.min(0.8, consecutiveFailures * 0.1);
  const baseCreativity = 0.2 + creativityBoost;
  
  const queryTemplates = {
    IT_GENERAL: [
      "indian IT companies hiring {year}",
      "software developer jobs india {month}",
      "tech jobs portal india list",
      "IT recruitment sites india",
      "programming jobs india {location}",
      "indian tech startups hiring",
      "software engineer careers india",
      "IT job boards india comprehensive",
      "indian companies tech hiring {year}",
      "developer jobs india remote"
    ],
    
    DATA_SCIENCE: [
      "data science jobs india portal",
      "machine learning careers india",
      "AI jobs india {year}",
      "data analyst hiring india",
      "ML engineer jobs indian companies",
      "data science job boards india",
      "AI startups india hiring",
      "analytics jobs india portal"
    ],
    
    STARTUP_ECOSYSTEM: [
      "indian startup job portals",
      "startup hiring platforms india",
      "unicorn companies india jobs",
      "indian startup jobs {year}",
      "startup career portals india",
      "new indian startups hiring",
      "indian startup job sites list"
    ],
    
    REGIONAL_FOCUS: [
      "bangalore IT jobs portal",
      "hyderabad tech jobs sites",
      "pune software jobs platforms",
      "chennai IT hiring portals",
      "noida tech jobs sites",
      "mumbai IT career portals",
      "ahmedabad tech job boards"
    ],
    
    UNCONVENTIONAL: [
      "hidden IT job portals india",
      "lesser known job sites india tech",
      "niche IT recruitment india",
      "exclusive tech job platforms india",
      "private IT job boards india",
      "boutique tech recruitment india"
    ]
  };
  
  const templates = queryTemplates[categoryFocus] || queryTemplates.IT_GENERAL;
  const currentDate = new Date();
  const currentYear = currentDate.getFullYear();
  const currentMonth = currentDate.toLocaleString('default', { month: 'long' });
  const indianCities = ['bangalore', 'hyderabad', 'pune', 'chennai', 'mumbai', 'delhi', 'noida', 'gurgaon'];
  
  return templates.map(template => {
    return template
      .replace('{year}', currentYear.toString())
      .replace('{month}', currentMonth)
      .replace('{location}', indianCities[Math.floor(Math.random() * indianCities.length)]);
  }).slice(0, DISCOVERY_BATCH_SIZE);
}

// --- ENHANCED PORTAL CLASSIFIER ---
async function classifyAndScorePortal(url: string, content: string): Promise<any> {
  const classificationPrompt = `
You are an expert job portal classifier specializing in Indian IT recruitment platforms.

Analyze this URL and content to determine:
1. Portal type (major portal, niche tech, company careers, startup platform, government, regional, freelance)
2. IT job relevance score (0-1)
3. Volume potential (estimated daily IT job postings: low/medium/high/massive)
4. Unique value (what makes this portal special)
5. Reliability score (0-1)

URL: ${url}
Content sample: ${content.substring(0, 5000)}

Respond with JSON:
{
  "portal_type": "string",
  "it_relevance_score": float,
  "volume_potential": "string",
  "daily_job_estimate": integer,
  "unique_value": "string",
  "reliability_score": float,
  "specializations": ["array", "of", "strings"],
  "geographic_focus": "string",
  "is_worth_monitoring": boolean
}`;

  try {
    const result = await callGeminiWithFallback(classificationPrompt);
    const jsonString = result.candidates[0].content.parts[0].text
      .replace(/```json/g, '').replace(/```/g, '').trim();
    return JSON.parse(jsonString);
  } catch (error) {
    console.warn(`Classification failed for ${url}: ${error.message}`);
    return null;
  }
}

// --- MULTI-THREADED DISCOVERY ENGINE ---
async function runMultiThreadedDiscovery(searchQueries: string[]) {
  const serpApiKeys = (Deno.env.get("SERPAPI_KEYS") || "").split(',').map(k => k.trim()).filter(Boolean);
  if (serpApiKeys.length === 0) throw new Error("SERPAPI_KEYS environment variable is not set.");
  
  const discoveryPromises = searchQueries.map(async (query, index) => {
    const apiKey = serpApiKeys[index % serpApiKeys.length];
    
    try {
      const searchRes = await fetch(
        `https://serpapi.com/search.json?q=${encodeURIComponent(query)}&api_key=${apiKey}&google_domain=google.co.in&gl=in&num=20`,
        { timeout: 10000 }
      );
      
      if (searchRes.ok) {
        const data = await searchRes.json();
        if (!data.error && data.organic_results) {
          return {
            query,
            results: data.organic_results.slice(0, 10), // Top 10 results per query
            success: true
          };
        }
      }
    } catch (error) {
      console.warn(`Search failed for "${query}": ${error.message}`);
    }
    
    return { query, results: [], success: false };
  });
  
  return await Promise.allSettled(discoveryPromises);
}

// --- INTELLIGENT PORTAL VERIFIER ---
async function runAdvancedVerifier(url: string): Promise<any> {
  try {
    const response = await fetch(url, { 
      headers: { 'User-Agent': BROWSER_USER_AGENT },
      timeout: 8000
    });
    
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    
    const content = await response.text();
    const verificationPrompt = `
You are an elite job portal verification specialist focusing on Indian IT opportunities.

Analyze this webpage to determine if it's a legitimate job portal or careers page with IT opportunities:

POSITIVE INDICATORS:
- Active job listings (especially IT/software/data science)
- Search/filter functionality for jobs
- Application processes
- Company profiles
- Salary information
- Job categories including technology
- Recent posting dates
- Professional design and structure

NEGATIVE INDICATORS:
- No actual job listings
- Broken functionality
- Outdated content (>6 months old)
- Spam or low-quality content
- Not focused on employment
- Requires payment for basic access

URL: ${url}
Content: ${content.substring(0, 40000)}

Respond with JSON:
{
  "is_job_portal": boolean,
  "confidence_score": float,
  "it_job_count_estimate": integer,
  "portal_quality": "low|medium|high|premium",
  "last_updated": "recent|moderate|old|unknown",
  "has_it_jobs": boolean,
  "portal_features": ["array of features"],
  "reasoning": "brief explanation",
  "final_url": "${url}"
}`;

    const result = await callGeminiWithFallback(verificationPrompt);
    const jsonString = result.candidates[0].content.parts[0].text
      .replace(/```json/g, '').replace(/```/g, '').trim();
    
    const verificationData = JSON.parse(jsonString);
    
    // Enhanced classification
    if (verificationData.is_job_portal && verificationData.confidence_score > PORTAL_CONFIDENCE_THRESHOLD) {
      const classification = await classifyAndScorePortal(url, content);
      return { ...verificationData, classification };
    }
    
    return verificationData;
    
  } catch (error) {
    console.warn(`Verification failed for ${url}: ${error.message}`);
    return {
      is_job_portal: false,
      confidence_score: 0,
      error: error.message,
      final_url: url
    };
  }
}

// --- ENHANCED SAVE SYSTEM WITH METADATA ---
async function saveVerifiedPortalWithMetadata(portalData: any): Promise<boolean> {
  try {
    const kv = await Deno.openKv();
    
    // Save to main verified portals list
    const mainKey = ["VERIFIED_JOB_PORTALS"];
    const currentPortals = (await kv.get<any[]>(mainKey)).value || [];
    
    const existingIndex = currentPortals.findIndex(p => p.url === portalData.final_url);
    
    const portalEntry = {
      url: portalData.final_url,
      discovered_at: new Date().toISOString(),
      confidence_score: portalData.confidence_score,
      it_job_count_estimate: portalData.it_job_count_estimate || 0,
      portal_quality: portalData.portal_quality || 'unknown',
      classification: portalData.classification || null,
      last_verified: new Date().toISOString(),
      verification_count: 1
    };
    
    if (existingIndex >= 0) {
      currentPortals[existingIndex] = {
        ...currentPortals[existingIndex],
        ...portalEntry,
        verification_count: (currentPortals[existingIndex].verification_count || 0) + 1
      };
    } else {
      currentPortals.push(portalEntry);
    }
    
    await kv.set(mainKey, currentPortals);
    
    // Save by category for efficient querying
    if (portalData.classification) {
      const categoryKey = ["PORTALS_BY_CATEGORY", portalData.classification.portal_type];
      const categoryPortals = (await kv.get<any[]>(categoryKey)).value || [];
      categoryPortals.push(portalEntry);
      await kv.set(categoryKey, categoryPortals);
    }
    
    // Update statistics
    const statsKey = ["PORTAL_DISCOVERY_STATS"];
    const stats = (await kv.get<any>(statsKey)).value || { total_discovered: 0 };
    stats.total_discovered = currentPortals.length;
    stats.last_discovery = new Date().toISOString();
    await kv.set(statsKey, stats);
    
    console.log(`✅ SAVED PORTAL: ${portalData.final_url} (Quality: ${portalData.portal_quality})`);
    return true;
    
  } catch (error) {
    console.error(`❌ Failed to save portal: ${error.message}`);
    return false;
  }
}

// --- ENHANCED MAIN ORCHESTRATOR ---
async function runVolumeMaximizedOrchestration() {
  console.log("\n🚀 === VOLUME MAXIMIZER MODE ACTIVATED ===");
  const kv = await Deno.openKv();
  
  // Load enhanced state
  let seedLibrary = (await kv.get<any>(["ENHANCED_SEED_LIBRARY"])).value || {};
  let analytics = (await kv.get<any>(["ENHANCED_ANALYTICS"])).value || {
    successfulPatterns: [], failedPatterns: [], consecutiveFailures: 0,
    totalDiscovered: 0, categoryPerformance: {}, lastEvolution: null
  };
  
  // Initialize seed library if empty
  if (Object.keys(seedLibrary).length === 0) {
    SEED_EXPANSION_STRATEGIES.forEach(url => {
      seedLibrary[url] = { 
        score: 1, 
        last_visited: "2000-01-01T00:00:00Z", 
        category: 'initial',
        success_rate: 0.5 
      };
    });
  }
  
  const initialPortalCount = ((await kv.get<any[]>(["VERIFIED_JOB_PORTALS"])).value || []).length;
  
  // Determine operation mode based on performance
  const shouldExplore = analytics.consecutiveFailures < 10 && Object.keys(seedLibrary).length > 0;
  
  if (shouldExplore) {
    console.log("📊 MODE: PARALLEL EXPLORATION & VERIFICATION");
    
    // Get eligible sources for exploration
    const twentyThreeHoursAgo = new Date(Date.now() - 23 * 60 * 60 * 1000);
    const eligibleSources = Object.entries(seedLibrary)
      .filter(([_, data]: [string, any]) => new Date(data.last_visited) < twentyThreeHoursAgo)
      .sort(([, a]: [string, any], [, b]: [string, any]) => b.score - a.score)
      .slice(0, MAX_PARALLEL_EXPLORATIONS);
    
    if (eligibleSources.length > 0) {
      console.log(`🔍 Exploring ${eligibleSources.length} sources in parallel...`);
      
      const explorationPromises = eligibleSources.map(async ([sourceUrl, sourceData]: [string, any]) => {
        try {
          seedLibrary[sourceUrl].last_visited = new Date().toISOString();
          
          const sourceResponse = await fetch(sourceUrl, { 
            headers: { 'User-Agent': BROWSER_USER_AGENT },
            timeout: 10000
          });
          
          if (!sourceResponse.ok) return { sourceUrl, companies: [] };
          
          const pageText = await sourceResponse.text();
          const extractionPrompt = `
Extract job portals, career sites, and company names from this content. Focus on:
- Job portal URLs and names
- IT companies with career pages
- Recruitment platforms
- Startup job boards

Content: ${pageText.substring(0, 50000)}

Respond with JSON:
{
  "job_portals": [{"name": "string", "url": "string", "type": "portal|company|platform"}],
  "companies": [{"name": "string", "likely_has_careers": boolean}]
}`;
          
          const result = await callGeminiWithFallback(extractionPrompt);
          const jsonString = result.candidates[0].content.parts[0].text
            .replace(/```json/g, '').replace(/```/g, '').trim();
          const extracted = JSON.parse(jsonString);
          
          return { sourceUrl, ...extracted };
          
        } catch (error) {
          console.warn(`Exploration failed for ${sourceUrl}: ${error.message}`);
          return { sourceUrl, job_portals: [], companies: [] };
        }
      });
      
      const explorationResults = await Promise.allSettled(explorationPromises);
      
      // Collect all URLs to verify
      const urlsToVerify = new Set<string>();
      
      explorationResults.forEach(result => {
        if (result.status === 'fulfilled' && result.value) {
          const { job_portals = [], companies = [] } = result.value;
          
          // Add direct portal URLs
          job_portals.forEach((portal: any) => {
            if (portal.url && portal.url.startsWith('http')) {
              urlsToVerify.add(portal.url);
            }
          });
          
          // Generate company career URLs
          companies.forEach((company: any) => {
            if (company.likely_has_careers) {
              const companyName = company.name.toLowerCase()
                .replace(/\s+/g, '')
                .replace(/[^a-z0-9]/g, '');
              
              // Try multiple career URL patterns
              const patterns = [
                `https://${companyName}.com/careers`,
                `https://${companyName}.com/jobs`,
                `https://careers.${companyName}.com`,
                `https://jobs.${companyName}.com`,
                `https://www.${companyName}.com/career`
              ];
              
              patterns.forEach(url => urlsToVerify.add(url));
            }
          });
        }
      });
      
      console.log(`🔍 Found ${urlsToVerify.size} URLs to verify in parallel...`);
      
      // Parallel verification with batching
      const urlArray = Array.from(urlsToVerify);
      const verificationBatches = [];
      
      for (let i = 0; i < urlArray.length; i += MAX_PARALLEL_VERIFICATIONS) {
        verificationBatches.push(urlArray.slice(i, i + MAX_PARALLEL_VERIFICATIONS));
      }
      
      let newPortalsFound = 0;
      
      for (const batch of verificationBatches) {
        const verificationPromises = batch.map(url => runAdvancedVerifier(url));
        const batchResults = await Promise.allSettled(verificationPromises);
        
        for (const result of batchResults) {
          if (result.status === 'fulfilled' && result.value) {
            const verificationData = result.value;
            
            if (verificationData.is_job_portal && 
                verificationData.confidence_score > PORTAL_CONFIDENCE_THRESHOLD) {
              
              const saved = await saveVerifiedPortalWithMetadata(verificationData);
              if (saved) newPortalsFound++;
            }
          }
        }
        
        // Small delay between batches to avoid overwhelming servers
        if (verificationBatches.indexOf(batch) < verificationBatches.length - 1) {
          await new Promise(resolve => setTimeout(resolve, 2000));
        }
      }
      
      analytics.totalDiscovered += newPortalsFound;
      if (newPortalsFound > 0) {
        analytics.consecutiveFailures = 0;
      } else {
        analytics.consecutiveFailures++;
      }
    }
  } else {
    console.log("🔄 MODE: ADAPTIVE DISCOVERY");
    
    // Multi-category discovery
    const categories = ['IT_GENERAL', 'DATA_SCIENCE', 'STARTUP_ECOSYSTEM', 'REGIONAL_FOCUS', 'UNCONVENTIONAL'];
    const focusCategory = categories[analytics.consecutiveFailures % categories.length];
    
    console.log(`🎯 Focus Category: ${focusCategory}`);
    
    const searchQueries = generateAdaptiveSearchQueries(analytics, focusCategory);
    const discoveryResults = await runMultiThreadedDiscovery(searchQueries);
    
    const newSeedUrls = new Set<string>();
    
    discoveryResults.forEach(result => {
      if (result.status === 'fulfilled' && result.value && result.value.success) {
        result.value.results.forEach((item: any) => {
          if (item.link && !seedLibrary[item.link]) {
            newSeedUrls.add(item.link);
          }
        });
      }
    });
    
    console.log(`🌱 Adding ${newSeedUrls.size} new seed URLs`);
    
    newSeedUrls.forEach(url => {
      seedLibrary[url] = {
        score: 1,
        last_visited: "2000-01-01T00:00:00Z",
        category: focusCategory,
        success_rate: 0.5,
        discovered_at: new Date().toISOString()
      };
    });
    
    if (newSeedUrls.size > 0) {
      analytics.consecutiveFailures = Math.max(0, analytics.consecutiveFailures - 2);
    } else {
      analytics.consecutiveFailures++;
    }
  }
  
  // Save enhanced state
  await kv.set(["ENHANCED_SEED_LIBRARY"], seedLibrary);
  await kv.set(["ENHANCED_ANALYTICS"], analytics);
  
  // Final report
  const finalPortalCount = ((await kv.get<any[]>(["VERIFIED_JOB_PORTALS"])).value || []).length;
  const newDiscoveries = finalPortalCount - initialPortalCount;
  
  console.log("\n📊 === CYCLE SUMMARY ===");
  console.log(`🆕 New portals discovered: ${newDiscoveries}`);
  console.log(`🏆 Total verified portals: ${finalPortalCount}`);
  console.log(`📈 Seed library size: ${Object.keys(seedLibrary).length}`);
  console.log(`❌ Consecutive failures: ${analytics.consecutiveFailures}`);
  
  if (newDiscoveries > 0) {
    console.log("🎉 SUCCESS! Volume maximization working!");
  }
}

// --- DENO DEPLOY SETUP ---
Deno.cron(
  "Volume Maximizer Job Bot",
  "*/8 * * * *", // More frequent execution for higher volume
  () => {
    console.log("⏰ Volume Maximizer triggered by cron");
    runVolumeMaximizedOrchestration().catch(console.error);
  }
);

Deno.serve(async (req) => {
  const url = new URL(req.url);
  
  if (url.pathname === '/stats') {
    const kv = await Deno.openKv();
    const portals = (await kv.get<any[]>(["VERIFIED_JOB_PORTALS"])).value || [];
    const analytics = (await kv.get<any>(["ENHANCED_ANALYTICS"])).value || {};
    
    return new Response(JSON.stringify({
      total_portals: portals.length,
      analytics,
      recent_portals: portals.slice(-10)
    }, null, 2), {
      headers: { 'Content-Type': 'application/json' }
    });
  }
  
  if (url.pathname === '/trigger') {
    console.log("👋 Manual volume maximizer trigger");
    runVolumeMaximizedOrchestration().catch(console.error);
    return new Response("Volume Maximizer triggered! Check logs for progress.");
  }
  
  return new Response("Volume Maximizer Bot - Use /trigger to start or /stats to view progress");
});
