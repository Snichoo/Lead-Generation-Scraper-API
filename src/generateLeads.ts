//stable

import { OpenAI } from "openai";
import { zodResponseFormat } from "openai/helpers/zod";
import { z } from "zod";
import fs from 'fs';
import * as path from 'path';
import axios from "axios";
import * as cheerio from "cheerio";
import pLimit from 'p-limit';
import { createObjectCsvStringifier } from 'csv-writer';
import { format } from 'date-fns';
import { suburbsByCity } from "./suburbs.js";

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// Schema to validate the response format from OpenAI
const LocationCheck = z.object({
  isBroadLocation: z.boolean(),
});

// Define the schema for the output structure
const PersonSchema = z.object({
  id: z.string(),
  title: z.string(),
});

// Extend the schema to include additional details
const EnrichedPersonSchema = z.object({
  id: z.string(),
  first_name: z.string(),
  last_name: z.string(),
  email: z.string().email(),
  title: z.string(),
  linkedin_url: z.string().url().optional(),
});

interface Organization {
  name?: string;
  domain?: string;
  website_url?: string;
  // Add other properties if needed
}

interface SearchResultPerson {
  id: string;
  title: string;
  organization: Organization;
  // Add other properties as needed
}

interface SearchResult {
  people: SearchResultPerson[];
  // Add other properties as needed
}

interface EnrichmentMatch {
  id: string;
  email?: string;
  first_name?: string;
  last_name?: string;
  title?: string;
  linkedin_url?: string;
  headline?: string;
  organization?: {
    name?: string;
    // Include other fields if necessary
  };
  // Add other properties as needed
}

interface EnrichmentResult {
  matches: EnrichmentMatch[];
  // Add other properties as needed
}

// Function to call the mixed people search API and return the highest role persons for multiple domains
async function getHighestRolePerson(
  organizationDomains: string[]
): Promise<{ id: string; title: string; domain: string }[]> {
  console.log("getHighestRolePerson called with domains:", organizationDomains);

  const searchUrl = "https://api.apollo.io/v1/mixed_people/search";

  const searchData = {
    q_organization_domains: organizationDomains.join("\n"), // Join domains by new line character
    page: 1,
    per_page: 100,
  };

  const headers = {
    "Cache-Control": "no-cache",
    "Content-Type": "application/json",
    "X-Api-Key": process.env.APOLLO_SEARCH_API_KEY || "",
  };

  try {
    // Step 1: Search for people in the organization domains
    const searchResponse = await fetch(searchUrl, {
      method: "POST",
      headers: headers,
      body: JSON.stringify(searchData),
    });

    if (!searchResponse.ok) {
      throw new Error(`HTTP error! status: ${searchResponse.status}`);
    }

    const searchResult: SearchResult = await searchResponse.json();

    if (!searchResult.people || searchResult.people.length === 0) {
      console.log(
        `No people found for domains: ${organizationDomains.join(", ")}`
      );
      return [];
    }

    // Group people by their organization domain
    const peopleByDomain: { [domain: string]: SearchResultPerson[] } = {};

    searchResult.people.forEach((person) => {
      const personDomain =
        person.organization?.domain ||
        (person.organization?.website_url
          ? new URL(person.organization.website_url).hostname
          : null);

// Inside the for-loop where you process each person
const personDomainRaw = person.organization?.domain || (person.organization?.website_url ? new URL(person.organization.website_url).hostname : null);

if (personDomainRaw) {
  const normalizedPersonDomain = getRootDomain(personDomainRaw.toLowerCase());
  peopleByDomain[normalizedPersonDomain] = peopleByDomain[normalizedPersonDomain] || [];
  peopleByDomain[normalizedPersonDomain].push(person);
} else {
  console.log("Person without organization domain:", person);
}

    });

    const highestRolePersons: { id: string; title: string; domain: string }[] =
      [];

    // For each domain, find the person with the highest role
    for (const domain of Object.keys(peopleByDomain)) {
      const people = peopleByDomain[domain];

      // Clean up the result by extracting only 'id' and 'title'
      const cleanedResults = people.map((person: SearchResultPerson) => ({
        id: person.id,
        title: person.title,
      }));

      console.log(`People for domain ${domain}:`, JSON.stringify(cleanedResults, null, 2));

      // Use GPT to find the person with the highest role
      const completion = await openai.beta.chat.completions.parse({
        model: "gpt-4o-2024-08-06",
        messages: [
          {
            role: "system",
            content:
              "You are a helpful assistant that identifies the person with the highest role in a company based on their title.",
          },
          {
            role: "user",
            content: `Given the following people and their titles: ${JSON.stringify(
              cleanedResults
            )}. Find the person with the highest role.`,
          },
        ],
        response_format: zodResponseFormat(PersonSchema, "highest_role_person"),
      });

      const highestRolePerson: { id: string; title: string } | null =
        completion.choices[0].message.parsed;

      console.log(`Highest role person for domain ${domain}:`, highestRolePerson);

      if (highestRolePerson) {
        highestRolePersons.push({ ...highestRolePerson, domain });
      } else {
        console.log(
          `Highest role person could not be determined for domain ${domain}.`
        );
      }
    }

    console.log("Final highest role persons:", highestRolePersons);

    return highestRolePersons;
  } catch (error) {
    console.error("Error with Apollo API request or GPT processing:", error);
    return [];
  }
}

async function enrichHighestRolePersons(
  highestRolePersons: { id: string; title: string; companyIndex: number }[],
  savedData: any[]
) {
  console.log("enrichHighestRolePersons called with highestRolePersons:", highestRolePersons);

  if (highestRolePersons.length === 0) {
    console.log("No highest role persons to enrich.");
    return;
  }

  const enrichmentData = {
    reveal_personal_emails: true,
    reveal_phone_number: false,
    details: highestRolePersons.map((person) => ({ id: person.id })),
  };

  const enrichmentUrl = "https://api.apollo.io/api/v1/people/bulk_match";

  const headers = {
    "Cache-Control": "no-cache",
    "Content-Type": "application/json",
    "X-Api-Key": process.env.APOLLO_BULK_MATCH_API_KEY || "",
  };

  try {
    console.log(
      "Starting bulk enrichment for persons:",
      highestRolePersons
    );
    console.log(
      "Enrichment data payload being sent:",
      JSON.stringify(enrichmentData, null, 2)
    );

    const enrichmentResponse = await fetch(enrichmentUrl, {
      method: "POST",
      headers: headers,
      body: JSON.stringify(enrichmentData),
    });

    if (!enrichmentResponse.ok) {
      const errorText = await enrichmentResponse.text();
      console.error(
        `HTTP error during enrichment! Status: ${enrichmentResponse.status} ${errorText}`
      );
      throw new Error(`HTTP error! status: ${enrichmentResponse.status}`);
    }

    const enrichmentResult: EnrichmentResult = await enrichmentResponse.json();

    // Log the enrichment results with only important fields
    console.log("Bulk Enrichment Results:");

    enrichmentResult.matches.forEach((match) => {
      const importantInfo = {
        first_name: match.first_name,
        last_name: match.last_name,
        title: match.title,
        headline: match.headline,
        email: match.email,
        organization_name: match.organization?.name || "",
      };
      console.log(importantInfo);
    });

    // Map enriched matches by ID for easy lookup
    const enrichedMatchesMap: { [id: string]: EnrichmentMatch } = {};
    enrichmentResult.matches.forEach((match) => {
      enrichedMatchesMap[match.id] = match;
    });


    // Update the corresponding companies in savedData
    highestRolePersons.forEach((person) => {
      const enrichedMatch = enrichedMatchesMap[person.id];
      if (enrichedMatch) {
        const company = savedData[person.companyIndex];
        if (!company) {
          console.error(
            `Company at index ${person.companyIndex} is undefined`
          );
          return; // or continue to the next iteration
        }
        company.first_name = enrichedMatch.first_name || "";
        company.last_name = enrichedMatch.last_name || "";
        company.company_personal_email = enrichedMatch.email || "";
        company.title = enrichedMatch.title || person.title;
        company.linkedin_url = enrichedMatch.linkedin_url || "";
        console.log(
          `Updated company at index ${person.companyIndex} with contact details:`,
          {
            first_name: company.first_name,
            last_name: company.last_name,
            company_personal_email: company.company_personal_email,
            title: company.title,
            linkedin_url: company.linkedin_url,
          }
        );
      } else {
        console.log(`Enriched data for person ID ${person.id} not found.`);
      }
    });
  } catch (error) {
    console.error("Error during bulk enrichment:", error);
  }
}


function extractSuburbOrCity(locationInput: string): string {
  // Regular expressions to match Australian states and territories
  const stateRegex = /\b(New South Wales|NSW|Victoria|VIC|Queensland|QLD|South Australia|SA|Western Australia|WA|Tasmania|TAS|Northern Territory|NT|Australian Capital Territory|ACT)\b/i;
  const stateRegexGlobal = /\b(New South Wales|NSW|Victoria|VIC|Queensland|QLD|South Australia|SA|Western Australia|WA|Tasmania|TAS|Northern Territory|NT|Australian Capital Territory|ACT)\b/gi;

  // Check if the input is a state
  const isStateOnly = locationInput.trim().match(stateRegexGlobal)?.length === 1 &&
    locationInput.trim().replace(stateRegexGlobal, "").trim() === "";

  if (isStateOnly) {
    throw new Error("Please provide a specific suburb or city, not just a state.");
  }

  // Split the address into components
  const addressComponents = locationInput.split(",");

  // Iterate from the end to the beginning
  for (let i = addressComponents.length - 1; i >= 0; i--) {
    let component = addressComponents[i].trim();

    // Skip if component is country or postcode
    if (component.toLowerCase() === "australia" || /^\d{4}$/.test(component)) {
      continue;
    }

    // Remove state abbreviations from the component
    const cleanedComponent = component.replace(stateRegexGlobal, "").trim();

    // If the cleaned component is not empty, return it
    if (cleanedComponent) {
      return cleanedComponent; // Removed ", Australia"
    }
  }

  throw new Error("Unable to extract a valid suburb or city from the input.");
}

async function scrapeGoogleMaps(
  businessType: string,
  location: string
): Promise<any[]> {
  const apiKey = process.env.RENDER_API_KEY || ""; // Include your API key if required

  const endpoint = "https://new-map-scraper-54137747006.us-central1.run.app/search";

  const requestData = {
    business_type: businessType,
    location: location,
  };

  const headers = {
    "Content-Type": "application/json",
    "X-API-Key": apiKey,
  };

  try {
    const response = await axios.post(endpoint, requestData, { headers, timeout: 300000 }); 

    if (response.status !== 200) {
      throw new Error(`Error fetching data: ${response.statusText}`);
    }

    const data = response.data;

    // Map the response to the expected format
    const results = data.map((item: any) => ({
      company_name: item.company_name,
      address: item.address,
      website: item.website || "",
      company_phone: item.company_phone || "",
    }));

    // Log the scraped data
    console.log(`Scraped data for location: ${location}`);
    console.log(JSON.stringify(results, null, 2)); // Pretty print the JSON result

    return results;
  } catch (error) {
    console.error("Error in scrapeGoogleMaps:", error);
    throw error;
  }
}

// Function to remove duplicates based on a unique key (e.g., phone or title)
function removeDuplicates(results: any[]): any[] {
  const seen = new Set();
  return results.filter((item) => {
    const identifier = item.company_phone || item.company_name; // Use phone or title as the unique identifier
    if (seen.has(identifier)) {
      return false; // If already seen, remove the duplicate
    }
    seen.add(identifier);
    return true; // Keep unique entries
  });
}

// Function to write the final JSON to a file
function saveToFile(filename: string, data: any) {
  const filepath = path.join(process.cwd(), "public", "csv", filename);
  fs.mkdirSync(path.dirname(filepath), { recursive: true });
  fs.writeFileSync(filepath, JSON.stringify(data, null, 2));
  console.log(`Saved JSON data to ${filepath}`);
}

function readJsonFromFile(filename: string): any[] {
  const filepath = path.join(process.cwd(), "public", "csv", filename);
  if (fs.existsSync(filepath)) {
    const data = fs.readFileSync(filepath, "utf-8");
    return JSON.parse(data);
  } else {
    return [];
  }
}

// Function to normalize URLs for consistent mapping
function normalizeUrl(urlStr: string): string {
  try {
    const parsedUrl = new URL(urlStr);
    // Remove www prefix
    if (parsedUrl.hostname.startsWith("www.")) {
      parsedUrl.hostname = parsedUrl.hostname.slice(4);
    }
    // Remove trailing slashes
    return parsedUrl.origin + parsedUrl.pathname.replace(/\/+$/, "");
  } catch (e) {
    return urlStr;
  }
}

async function runActorPool(
  businessType: string,
  suburbs: string[],
  maxConcurrency: number,
  stateAbbr: string  // Accept the state abbreviation here
): Promise<any[]> {
  const allResults: any[] = [];
  const allPromises: Promise<void>[] = [];
  let activeCount = 0;
  let nextIndex = 0;

  return new Promise<void>((resolve, reject) => {
    function runNextActor() {
      while (activeCount < maxConcurrency && nextIndex < suburbs.length) {
        let suburb = suburbs[nextIndex++].trim();
        activeCount++;

        // Check if suburb already ends with the state abbreviation
        const suburbLower = suburb.toLowerCase();
        if (!suburbLower.endsWith(` ${stateAbbr.toLowerCase()}`)) {
          suburb = `${suburb} ${stateAbbr}`;
        }

        console.log(`Starting actor for suburb: ${suburb}`);
        const actorPromise = scrapeGoogleMaps(businessType, suburb)
          .then((results) => {
            console.log(`Actor completed for suburb: ${suburb}`);
            allResults.push(...results);
          })
          .catch((error) => {
            console.error(`Error running actor for suburb: ${suburb}`, error);
            reject
          })
          .finally(() => {
            activeCount--;
            if (activeCount === 0 && nextIndex >= suburbs.length) {
              resolve();
            } else {
              runNextActor();
            }
          });
        allPromises.push(actorPromise);
      }
    }

    runNextActor();
  }).then(() => Promise.all(allPromises).then(() => allResults))
}


function sanitizeFilename(name: string): string {
  return name.replace(/[^a-z0-9_-]/gi, "_").toLowerCase();
}

// Function to parse address into components
function parseAddress(address: string) {
  const regex = /^(.+?),\s*([^,]+)\s+(\w{2,3})\s+(\d{4}),\s*Australia$/;
  const match = address.match(regex);
  if (match) {
    return {
      streetAddress: match[1].trim(),
      suburb: match[2].trim(),
      postcode: match[4].trim(),
    };
  } else {
    return {
      streetAddress: "",
      suburb: "",
      postcode: "",
    };
  }
}

// Replace your existing generateCSVData function with this
// Replace your existing generateCSVFile function with this
async function generateCSVFile(
  businessType: string,
  location: string,
  data: any[]
): Promise<{ filename: string; fileSizeInBytes: number } | null> {
  if (data.length === 0) {
    console.log("No leads were found. Try changing locations or business type.");
    return null;
  }

  // Define the CSV columns
  const csvStringifier = createObjectCsvStringifier({
    header: [
      { id: "title", title: "Job Title" },
      { id: "first_name", title: "First Name" },
      { id: "last_name", title: "Last Name" },
      { id: "personal_email", title: "Personal Email Address" },
      { id: "company_email", title: "Company Email Address" },
      // Added new column for merged emails
      { id: "merged_email", title: "Merged Email" },
      { id: "phone_number", title: "Phone Number" },
      { id: "linkedin", title: "LinkedIn" },
      { id: "website", title: "Website" },
      { id: "company_name", title: "Company Name" },
      { id: "street_address", title: "Street No and Name" },
      { id: "address_suburb", title: "Address Suburb" },
      { id: "address_postcode", title: "Address Postcode" },
      { id: "postal_address", title: "Postal Address" },
      { id: "postal_suburb", title: "Postal Suburb" },
      { id: "postal_postcode", title: "Postal PostCode" },
      { id: "country", title: "Country" },
    ],
  });

  // Map the JSON data to CSV data
  const csvData = data.map((item) => {
    const addressParts = parseAddress(item.address || "");

    return {
      title: item.title || "",
      first_name: item.first_name || "",
      last_name: item.last_name || "",
      personal_email: item.company_personal_email || "",
      company_email: item.company_general_email || "",
      // Merging both emails into one column
      merged_email: [item.company_personal_email, item.company_general_email]
        .filter(Boolean)
        .join("; "),
      phone_number: item.company_phone || "",
      linkedin: item.linkedin_url || "",
      website: item.website || "",
      company_name: item.company_name || "",
      street_address: addressParts.streetAddress || "",
      address_suburb: addressParts.suburb || "",
      address_postcode: addressParts.postcode || "",
      postal_address: addressParts.streetAddress || "",
      postal_suburb: addressParts.suburb || "",
      postal_postcode: addressParts.postcode || "",
      country: "Australia",
    };
  });

  // Generate the CSV string
  const header = csvStringifier.getHeaderString();
  const records = csvStringifier.stringifyRecords(csvData);
  const csvContent = header + records;

  // Sanitize business type and location for filename
  const sanitizedBusinessType = sanitizeFilename(businessType);
  const sanitizedLocation = sanitizeFilename(location);

  // Get current date and time (excluding seconds)
  const now = new Date();
  const timestamp = format(now, 'yyyy-MM-dd_HH-mm');

  const filename = `${sanitizedBusinessType}_${sanitizedLocation}_${timestamp}.csv`;

  // Define the CSV file path
  const filepath = path.join('/tmp', 'csv_files', filename); // Use '/tmp' instead of process.cwd()

  // Ensure the directory exists
  fs.mkdirSync(path.dirname(filepath), { recursive: true });

  // Save the CSV file
  fs.writeFileSync(filepath, csvContent);

  console.log(`CSV file saved to ${filepath}`);

  // Get the file size
  const stats = fs.statSync(filepath);
  const fileSizeInBytes = stats.size;

  return { filename, fileSizeInBytes }; // Return the filename and file size
}


// Function to filter large companies using Perplexity and remove them from savedData
async function filterLargeCompanies(companies: any[]): Promise<string[]> {
  // Prepare the company list with IDs
  const companyList = companies
    .map((company) => `${company.id}: ${company.company_name}`)
    .join("\n");

  const options = {
    method: "POST",
    headers: {
      Authorization: `Bearer ${
        process.env.PERPLEXITY_API_KEY || ""
      }`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: "llama-3.1-sonar-huge-128k-online",
      messages: [
        {
          role: "system",
          content:
            "Imagine you are a small business broker. You get a list of companies to cold email. But some of the companies in the list are big companies, franchises etc that a small business broker shouldn't waste time emailing. I want you to identify if there are any of those kind of companies and if there are then mention them in your output. You must refer to them using their ID in the output. Don't mention other companies.",
        },
        {
          role: "user",
          content: companyList,
        },
      ],
      temperature: 0.2,
      top_p: 0.9,
      return_citations: false,
      search_domain_filter: ["perplexity.ai"],
      return_images: false,
      return_related_questions: false,
      search_recency_filter: "month",
      top_k: 0,
      stream: false,
      presence_penalty: 0,
      frequency_penalty: 1,
    }),
  };

  try {
    const response = await fetch(
      "https://api.perplexity.ai/chat/completions",
      options
    );

    if (!response.ok) {
      throw new Error(`HTTP error! Status: ${response.status}`);
    }

    const data = await response.json();
    const messageContent = data.choices[0].message.content;

    console.log("Perplexity Output:", messageContent);

    // Now process messageContent with ChatGPT to extract IDs
    const largeCompanyIds = await extractLargeCompanyIds(messageContent);

    return largeCompanyIds;
  } catch (err) {
    console.error("Error fetching Perplexity output:", err);
    throw err;
  }
}

// Function to extract large company IDs from Perplexity output using ChatGPT
async function extractLargeCompanyIds(
  perplexityOutput: string
): Promise<string[]> {
  const LargeCompaniesSchema = z.object({
    ids: z.array(z.string()),
  });

  const completion = await openai.beta.chat.completions.parse({
    model: "gpt-4o-2024-08-06",
    messages: [
      {
        role: "system",
        content:
          "Extract the IDs of the large companies mentioned in the text. Provide the IDs in a JSON format with key 'ids', which is an array of strings.",
      },
      {
        role: "user",
        content: perplexityOutput,
      },
    ],
    response_format: zodResponseFormat(
      LargeCompaniesSchema,
      "large_companies"
    ),
  });

  const result = completion.choices[0].message.parsed;

  if (result && result.ids) {
    return result.ids;
  } else {
    console.error("Failed to extract large company IDs from GPT response.");
    return [];
  }
}

// Function to extract emails from text
function extractEmails(text: string): string[] {
  const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,}/gi;
  let emails: string[] = text.match(emailRegex) || [];

  // Blacklist certain file extensions to filter out false positives
  const blacklistedExtensions = [
    ".jpg",
    ".jpeg",
    ".png",
    ".svg",
    ".gif",
    ".tga",
    ".bmp",
    ".zip",
    ".pdf",
    ".webp",
  ];

  emails = emails.filter((email) => {
    // Convert email and extensions to lowercase for case-insensitive comparison
    const lowerEmail = email.toLowerCase();
    return !blacklistedExtensions.some((ext) => lowerEmail.endsWith(ext));
  });

  return emails;
}

// Function to extract links from HTML
function extractLinks(html: string, baseUrl: string): string[] {
  const $ = cheerio.load(html);
  const links: string[] = [];
  $("a[href]").each((i, elem) => {
    let href = $(elem).attr("href");
    if (href) {
      // Remove URL fragments
      href = href.split("#")[0];
      // Trim whitespace
      href = href.trim();
      // Skip mailto, javascript links, empty or invalid hrefs
      if (
        href.startsWith("mailto:") ||
        href.startsWith("javascript:") ||
        href === "" ||
        href === "/" ||
        href === "https://" ||
        href === "http://" ||
        href === "//"
      ) {
        return;
      }
      // Resolve relative URLs
      try {
        const resolvedUrl = new URL(href, baseUrl).toString();
        // Ensure the link is on the same domain
        if (resolvedUrl.startsWith(baseUrl)) {
          links.push(resolvedUrl);
        }
      } catch (error) {
        // Suppress error logging to prevent terminal clutter
        // You can enable logging by uncommenting the line below
        // console.warn(`Invalid URL encountered: ${href} - ${error.message}`);
        // Skip invalid URLs
      }
    }
  });
  return links;
}

// Function to fetch a page's HTML content
async function fetchPage(pageUrl: string): Promise<string | null> {
  try {
    const response = await axios.get(pageUrl, {
      headers: {
        "User-Agent": "Mozilla/5.0 (compatible; EmailScraper/1.0)",
      },
      timeout: 10000,
      responseType: 'text', // Add this line
    });
    return response.data;
  } catch (error) {
    // Handle errors silently
    return null;
  }
}

// Function to crawl a website and find emails
async function crawlWebsite(startUrl: string): Promise<string[]> {
  const emailsFound = new Set<string>();
  const visited = new Set<string>();
  const queue: string[] = [];

  const maxPages = 40;
  let pagesCrawled = 0;
  let emailFound = false;

  const parsedUrl = new URL(startUrl);
  const baseUrl = `${parsedUrl.protocol}//${parsedUrl.host}`;

  // Start with the main page
  queue.push(startUrl);

  // Prepare potential contact page URLs
  const contactPaths = [
    "/contact",
    "/contact-us",
    "/contactus",
    "/about",
    "/about-us",
    "/aboutus",
    "/impressum",
  ];
  for (let path of contactPaths) {
    queue.push(new URL(path, baseUrl).toString());
  }

  while (queue.length > 0 && pagesCrawled < maxPages && !emailFound) {
    const currentUrl = queue.shift();

    if (!currentUrl) continue;

    if (visited.has(currentUrl)) {
      continue;
    }
    visited.add(currentUrl);

    const html = await fetchPage(currentUrl);
    if (!html) {
      continue;
    }
    pagesCrawled++;

    // Extract emails from page
    const emails = extractEmails(html);
    if (emails.length > 0) {
      emails.forEach((email) => emailsFound.add(email));
      emailFound = true;
      break; // Stop crawling this website
    }

    // Extract links from page
    const links = extractLinks(html, baseUrl);
    for (let link of links) {
      if (!visited.has(link)) {
        queue.push(link);
      }
    }
  }

  return Array.from(emailsFound);
}

async function checkLocation(location: string): Promise<string> {
  console.log(`Location to check: ${location}`);

  const broadLocations = [
    "sydney",
    "melbourne",
    "brisbane",
    "perth",
    "adelaide",
    "gold coast",
    "newcastle",
    "canberra",
    "wollongong",
    "geelong",
    "hobart",
    "townsville",
    "cairns",
    "toowoomba",
    "darwin",
    "ballarat",
    "bendigo",
    "albury–wodonga",
    "launceston",
    "mackay",
    "rockhampton",
    "bunbury",
  ];

  try {
    // Normalize location
    let normalizedLocation = location.toLowerCase();

    // Remove 'Australia' and state abbreviations from the location
    normalizedLocation = normalizedLocation.replace(
      /\b(australia|nsw|vic|qld|sa|wa|tas|nt|act)\b/gi,
      ''
    );

    // Remove any punctuation
    normalizedLocation = normalizedLocation.replace(/[^\w\s-]/gi, '');

    // Trim whitespace
    normalizedLocation = normalizedLocation.trim();

    // Check if normalized location matches any of the broad locations
    if (broadLocations.includes(normalizedLocation)) {
      return 'yes';
    } else {
      return 'no';
    }
  } catch (error) {
    console.error("Error during location check:", error);
    return 'no'; // Default to 'no' in case of error
  }
}


function normalizeCityName(cityName: string): string {
  return cityName
    .toLowerCase()
    .replace(/[\s–-]+/g, '_') // Replace spaces and dashes with underscores
    .replace(/[^\w_]/g, '');   // Remove any non-word characters except underscores
}

// Add this mapping near the top of your file or in a separate module
const stateByCity: { [key: string]: string } = {
  sydney: 'NSW',
  melbourne: 'VIC',
  brisbane: 'QLD',
  perth: 'WA',
  adelaide: 'SA',
  gold_coast: 'QLD',
  newcastle: 'NSW',
  canberra: 'ACT',
  wollongong: 'NSW',
  geelong: 'VIC',
  hobart: 'TAS',
  townsville: 'QLD',
  cairns: 'QLD',
  toowoomba: 'QLD',
  darwin: 'NT',
  ballarat: 'VIC',
  bendigo: 'VIC',
  albury_wodonga: 'VIC',
  launceston: 'TAS',
  mackay: 'QLD',
  rockhampton: 'QLD',
  bunbury: 'WA'
};

function getRootDomain(domain: string): string {
  const publicSuffixes = ['com', 'org', 'net', 'edu', 'gov', 'au', 'co'];
  const domainParts = domain.split('.');

  // Handle domains with known public suffixes (e.g., 'com.au')
  for (let i = domainParts.length - 1; i >= 0; i--) {
    if (!publicSuffixes.includes(domainParts[i])) {
      return domainParts.slice(i).join('.');
    }
  }

  // If no known public suffix is found, return the domain as is
  return domain;
}

// At the top of your generateLeads.ts file

// At the top of your generateLeads.ts file

const excludedDomains = new Set<string>([
  'google.com',
  'facebook.com',
  'linkedin.com',
  'amazon.com',
  'microsoft.com',
  'apple.com',
  'instagram.com',
  'twitter.com',
  'youtube.com',
  'wikipedia.org',
  'yahoo.com',
  'bing.com',
  'baidu.com',
  'tencent.com',
  'alibaba.com',
  'reddit.com',
  // Add more big company domains as needed
]);

function isDomainExcluded(domain: string): boolean {
  return excludedDomains.has(domain);
}

interface CompanyData {
  id: string;
  company_name: string;
  address: string;
  website?: string;
  company_phone?: string;
  company_personal_email?: string;
  company_general_email?: string;
  first_name?: string;
  last_name?: string;
  title?: string;
  linkedin_url?: string;
  [key: string]: any; // For additional properties
}



// In your generateLeads function, use generateCSVFile instead of generateCSVData
// In your generateLeads function, add more console logs
export async function generateLeads(
  businessType: string,
  locationInput: string
): Promise<{ filename?: string; fileSizeInBytes?: number; error?: string }> {
  try {
    // Extract the suburb or city from the user's location input
    const extractedLocation = extractSuburbOrCity(locationInput);
    console.log("Extracted Location:", extractedLocation);

    // Use the extracted location in the rest of the function
    const locationCheckResult = await checkLocation(extractedLocation);
    console.log("Location check result:", locationCheckResult);

    let uniqueResults;

    if (locationCheckResult === "yes") {
      console.log("Broad location detected, fetching list of suburbs...");
      let structuredSuburbs = [];

      const cityKey = normalizeCityName(extractedLocation);

      if (suburbsByCity[cityKey]) {
        structuredSuburbs = suburbsByCity[cityKey];
        // Get the state abbreviation for the city
        const stateAbbr = stateByCity[cityKey];
        if (!stateAbbr) {
          throw new Error(`State abbreviation for ${extractedLocation} not found.`);
        }
  
        console.log("Structured Suburb List:", structuredSuburbs);
  
        // Pass the state abbreviation to runActorPoolj
        uniqueResults = await runActorPool(
          businessType,
          structuredSuburbs,
          40,
          stateAbbr  // Pass the state abbreviation here
        );
      } else {
        throw new Error(`Suburbs list for ${extractedLocation} not available.`);
      }

      // Remove duplicates from the combined resultsk
      uniqueResults = removeDuplicates(uniqueResults);
    } else {
      console.log("Specific location detected, scraping Google Maps...");
    
      // Attempt to find the state abbreviation for the extracted location
      const cityKey = normalizeCityName(extractedLocation);
      let stateAbbr = stateByCity[cityKey];
    
      let locationToUse = extractedLocation;
      if (stateAbbr) {
        locationToUse = `${extractedLocation} ${stateAbbr}`;
      } else {
        console.warn(`State abbreviation for ${extractedLocation} not found. Using default location.`);
      }
    
      const results = await scrapeGoogleMaps(businessType, locationToUse);
    
      // Remove duplicates in case of single-location scraping
      uniqueResults = removeDuplicates(results);
    }

    // Save final results to JSON file
    saveToFile("finalResults.json", uniqueResults);

    // Read saved JSON file and assign IDs to each company
    let savedData: CompanyData[] = readJsonFromFile("finalResults.json");


    // Check if no leads were found after initial scraping
    if (savedData.length === 0) {
      console.log("No leads were found. Try changing locations or business type.");
      // Delete or clear csvFileInfo.json to prevent using old CSV files
      const csvFileInfoPath = path.join(
        process.cwd(),
        "public",
        "csv",
        "csvFileInfo.json"
      );
      if (fs.existsSync(csvFileInfoPath)) {
        fs.unlinkSync(csvFileInfoPath); // Delete the file
      }
      return { error: "No leads were found. Try changing locations or business type." };
    }

    // Assign an id to each company
    for (let i = 0; i < savedData.length; i++) {
      // Generate an id (not too short)
      savedData[i].id = `company_${i}_${Date.now()}`;
    }

    // Save updated savedData back to finalResults.json
    saveToFile("finalResults.json", savedData);

    console.log("Saved Data after assigning IDs:", JSON.stringify(savedData, null, 2));

    // New Step: Filter large companies using Perplexity
    console.log("Filtering large companies using Perplexity...");
    const batchSize = 30;
    const largeCompanyIds = new Set<string>(); // Use a Set to store unique IDs

    const limit = pLimit(5); // Limit concurrency to 5
    const batchPromises = [];

    for (let i = 0; i < savedData.length; i += batchSize) {
      const batch = savedData.slice(i, i + batchSize);
      const promise = limit(async () => {
        try {
          const ids = await filterLargeCompanies(batch);
          return ids;
        } catch (error) {
          console.error("Error in batch processing:", error);
          return []; // Return empty array on error to continue processing other batches
        }
      });
      batchPromises.push(promise);
    }

    // Now, await all promises
    const batchResults = await Promise.all(batchPromises);

    // Collect all IDs
    batchResults.forEach((ids) => {
      ids.forEach((id) => largeCompanyIds.add(id));
    });

    console.log("Large company IDs to remove:", Array.from(largeCompanyIds));

    // Now, remove the companies with IDs in largeCompanyIds from savedData
    savedData = savedData.filter((company) => !largeCompanyIds.has(company.id));

    // Save the updated savedData back to finalResults.json
    saveToFile("finalResults.json", savedData);

    console.log("Saved Data after filtering large companies:", JSON.stringify(savedData, null, 2));

    // Proceed with the rest of the lead generation process using the filtered savedData


    const highestRolePersons: {
      id: string;
      title: string;
      domain: string;
      companyIndex: number;
    }[] = [];
    
    let domainsBatch: string[] = [];
    let domainToCompanyIndex: { [domain: string]: number } = {};
    
    for (let index = 0; index < savedData.length; index++) {
      const company: CompanyData = savedData[index];
      console.log(`Processing company at index ${index}:`, company);
    
      if (company.website) {
        const websiteDomain: string = getRootDomain(
          new URL(company.website).hostname.toLowerCase()
        );
    
        if (isDomainExcluded(websiteDomain)) {
          console.log(`Excluded domain ${websiteDomain} from processing.`);
          continue; // Skip this company
        }
    
        domainsBatch.push(websiteDomain);
        domainToCompanyIndex[websiteDomain] = index;
    
        // When we have collected enough domains, process them
        if (domainsBatch.length === 10) {
          console.log("Processing domains batch:", domainsBatch);
          const highestRolePersonsBatch = await getHighestRolePerson(domainsBatch);
    
          console.log("Highest role persons found:", highestRolePersonsBatch);

          for (const person of highestRolePersonsBatch) {
            const normalizedDomain = getRootDomain(person.domain.toLowerCase());
            const companyIndex = domainToCompanyIndex[normalizedDomain];
          
            if (companyIndex === undefined) {
              console.error(`Company index not found for domain ${normalizedDomain}`);
            } else {
              highestRolePersons.push({ ...person, companyIndex });

              // When we have collected 10 highest role persons, enrich them
              if (highestRolePersons.length === 10) {
                console.log("Enriching highest role persons:", highestRolePersons);
                await enrichHighestRolePersons(highestRolePersons, savedData);
                highestRolePersons.length = 0; // Reset the array
              }
            }
          }

          // Clear the domainsBatch and domainToCompanyIndex
          domainsBatch = [];
          domainToCompanyIndex = {};
        }
      }
    }

    // Process any remaining domains
    if (domainsBatch.length > 0) {
      console.log("Processing remaining domains batch:", domainsBatch);
      const highestRolePersonsBatch = await getHighestRolePerson(domainsBatch);
    
      console.log("Highest role persons found:", highestRolePersonsBatch);

      for (const person of highestRolePersonsBatch) {
        const normalizedDomain = person.domain.toLowerCase().replace(/^www\./, "");
        const companyIndex = domainToCompanyIndex[normalizedDomain];

        if (companyIndex === undefined) {
          console.error(
            `Company index not found for domain ${normalizedDomain}`
          );
        } else {
          highestRolePersons.push({ ...person, companyIndex });

          // Enrich when we have 10 records
          if (highestRolePersons.length === 10) {
            console.log("Enriching highest role persons:", highestRolePersons);
            await enrichHighestRolePersons(highestRolePersons, savedData);
            highestRolePersons.length = 0; // Reset the array
          }
        }
      }

      // Clear the domainsBatch and domainToCompanyIndex
      domainsBatch = [];
      domainToCompanyIndex = {};
    }

    // Enrich any remaining highest role persons less than 10
    if (highestRolePersons.length > 0) {
      console.log("Enriching remaining highest role persons:", highestRolePersons);
      await enrichHighestRolePersons(highestRolePersons, savedData);
    }

    // Save the updated savedData back to finalResults.json
    saveToFile("finalResults.json", savedData);

    // --- New Step: Find company emails for companies without email ---

    // Identify companies without both personal and general emails and with website
    const companiesWithoutEmail = [];

    for (let index = 0; index < savedData.length; index++) {
      const company = savedData[index];
      if (
        (!company.company_personal_email ||
          company.company_personal_email.trim() === "") &&
        (!company.company_general_email ||
          company.company_general_email.trim() === "") &&
        company.website
      ) {
        const websiteDomain = new URL(company.website)
          .hostname.replace(/^www\./, "");
        companiesWithoutEmail.push({
          index,
          website: company.website,
          domain: websiteDomain,
        });
      }
    }

    console.log("Companies without email to process:", companiesWithoutEmail);

    // Proceed to process companies without email if any
    if (companiesWithoutEmail.length > 0) {
      console.log("Running email scraper for companies without email...");

      const limit = pLimit(5); // Limit concurrency to 5

      const crawlPromises = companiesWithoutEmail.map((company) =>
        limit(async () => {
          const emails = await crawlWebsite(company.website);
          if (emails.length > 0) {
            // Update the company in savedData
            const companyIndex = company.index;
            const companyInSavedData = savedData[companyIndex];
            if (
              !companyInSavedData.company_general_email ||
              companyInSavedData.company_general_email.trim() === ""
            ) {
              companyInSavedData.company_general_email = emails[0]; // Take the first email
              console.log(
                `Added general email to company at index ${companyIndex}: ${companyInSavedData.company_general_email}`
              );
            }
          } else {
            console.log(`No emails found for website: ${company.website}`);
          }
        })
      );

      await Promise.all(crawlPromises);

      // Save the updated savedData back to finalResults.json
      saveToFile("finalResults.json", savedData);
    } else {
      console.log("No companies without email found.");
    }

    // Generate the CSV file
    const csvResult = await generateCSVFile(businessType, extractedLocation, savedData);

    if (!csvResult) {
      return { error: "No leads were found. Try changing locations or business type." };
    }

    // Return the filename and file size
    return { filename: csvResult.filename, fileSizeInBytes: csvResult.fileSizeInBytes };



  } catch (error) {
    console.error("Error in the lead generation process:", error);
    return { error: "Lead generation failed" };
  }
}