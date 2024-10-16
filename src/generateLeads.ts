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
import Bottleneck from 'bottleneck';
import { ApifyClient } from 'apify-client';

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// Schema to validate the response format from OpenAI
const LocationCheck = z.object({
  isBroadLocation: z.boolean(),
});

// Define the schema for the output structure
const PersonSchema = z.object({
  name: z.string(),
  job_title: z.string(),
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
}

interface SearchResultPerson {
  id: string;
  first_name: string;
  last_name: string;
  title: string;
  organization_id: string;
  organization: Organization;
}

interface SearchResult {
  people: SearchResultPerson[];
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
  };
}

interface EnrichmentResult {
  matches: EnrichmentMatch[];
}

// Place 'getRootDomain' function near the top
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

// Define Contact interface
interface Contact {
  name: string;
  job_title: string;
  linkedin: string | null;
  number_of_employees: string;
}

interface HighestRolePerson {
  id: string;
  first_name: string;
  last_name: string;
  email: string;
  title: string;
  linkedin_url: string;
  domain: string;
  companyIndex: number;
}

async function getContactsFromApify(domains: string[]): Promise<{ [domain: string]: any[] }> {
  const contactsByDomain: { [domain: string]: any[] } = {};
  const client = new ApifyClient({
    token: process.env.APIFY_API_TOKEN,
  });

  const actorId = 'jljBwyyQakqrL1wae';

  const maxConcurrent = 60; // Adjust concurrency as appropriate
  const limit = pLimit(maxConcurrent);

  const tasks = domains.map(domain => limit(async () => {
    const url = `https://app.apollo.io/#/people?sortAscending=false&sortByField=%5Bnone%5D&page=1&qKeywords=${encodeURIComponent(domain)}`;
    const input = {
      url: url,
      totalRecords: 50,
      getWorkEmails: true,
      getPersonalEmails: true,
    };

    try {
      const run = await client.actor(actorId).call(input);

      // Fetch and print Actor results from the run's dataset (if any)
      const { items } = await client.dataset(run.defaultDatasetId).listItems();
      contactsByDomain[domain] = items;
    } catch (error) {
      console.error(`Error processing domain ${domain}:`, error);
      contactsByDomain[domain] = [];
    }
  }));

  await Promise.all(tasks);

  return contactsByDomain;
}

function delayPromise(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function extractSuburbOrCity(locationInput: string): string {
  // Regular expressions to match Australian states and territories
  const stateRegex = /\b(New South Wales|NSW|Victoria|VIC|Queensland|QLD|South Australia|SA|Western Australia|WA|Tasmania|TAS|Northern Territory|NT|Australian Capital Territory|ACT)\b/gi;

  // Remove 'Australia' from the input
  let cleanedInput = locationInput.replace(/\bAustralia\b/i, '').trim();

  // Now, extract the state abbreviation
  let stateMatch = cleanedInput.match(stateRegex);
  let stateAbbreviation = stateMatch ? stateMatch[0] : '';

  // Remove the state from the cleanedInput to get the suburb or city
  let suburbOrCity = cleanedInput.replace(stateRegex, '').replace(/,/g, '').trim();

  if (!suburbOrCity) {
    throw new Error("Unable to extract a valid suburb or city from the input.");
  }

  if (stateAbbreviation) {
    return `${suburbOrCity} ${stateAbbreviation}`;
  } else {
    return suburbOrCity;
  }
}

async function scrapeGoogleMaps(
  businessType: string,
  location: string,
  leadCount?: number
): Promise<any[]> {
  const apiKey = process.env.RENDER_API_KEY || ""; // Include your API key if required

  const endpoint = "https://new-map-scraper-54137747006.us-central1.run.app/search";

  const requestData = {
    business_type: businessType,
    location: location,
    lead_count: leadCount,
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

// Function to delay execution for a given number of milliseconds
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// Ensure 'runActorPool' function is corrected
async function runActorPool(
  businessType: string,
  suburbs: string[],
  maxConcurrency: number,
  stateAbbr: string,
  leadCount?: number
): Promise<any[]> {
  const allResults: any[] = [];
  const limit = pLimit(maxConcurrency);
  const tasks: Promise<void>[] = [];
  let totalLeadsCollected = 0;

  for (let i = 0; i < suburbs.length; i++) {
    if (leadCount && totalLeadsCollected >= leadCount) {
      break;
    }

    let suburb = suburbs[i].trim();

    // Check if suburb already ends with the state abbreviation
    const suburbLower = suburb.toLowerCase();
    if (!suburbLower.endsWith(` ${stateAbbr.toLowerCase()}`)) {
      suburb = `${suburb} ${stateAbbr}`;
    }

    console.log(`Starting actor for suburb: ${suburb}`);

    // Start the request with p-limit to control concurrency
    const task = limit(async () => {
      if (leadCount && totalLeadsCollected >= leadCount) {
        return;
      }
      try {
        const results = await scrapeGoogleMaps(businessType, suburb, leadCount);

        if (leadCount) {
          const remainingLeads = leadCount - totalLeadsCollected;
          const limitedResults = results.slice(0, remainingLeads);
          allResults.push(...limitedResults);
          totalLeadsCollected += limitedResults.length;
        } else {
          allResults.push(...results);
          totalLeadsCollected += results.length;
        }
      } catch (error) {
        console.error(`Error running actor for suburb: ${suburb}`, error);
      }
    });

    tasks.push(task);

    // Determine the delay
    const delayBetweenRequests = i < 20 ? 5000 : 1000;

    await delay(delayBetweenRequests);
  }

  // Wait for all tasks to complete
  await Promise.all(tasks);

  return allResults;
}

function sanitizeFilename(name: string): string {
  return name.replace(/[^a-z0-9_-]/gi, "_").toLowerCase();
}

// Function to parse address into components
function parseAddress(address: string) {
  // Remove 'Australia' from the end if present
  address = address.replace(/,\s*Australia$/i, '').trim();

  // Split the address by commas
  const parts = address.split(',');

  // Initialize variables
  let streetAddress = '';
  let suburb = '';
  let postcode = '';

  if (parts.length >= 1) {
    streetAddress = parts[0].trim();
  }

  if (parts.length >= 2) {
    // Extract suburb and possible postcode
    const suburbPart = parts[1].trim();
    const postcodeMatch = suburbPart.match(/(\D+)\s+(\d{4})$/);

    if (postcodeMatch) {
      suburb = postcodeMatch[1].trim();
      postcode = postcodeMatch[2].trim();
    } else {
      suburb = suburbPart;
    }
  }

  if (parts.length >= 3 && !postcode) {
    // If postcode wasn't found earlier, check the third part
    const postcodePart = parts[2].trim();
    const postcodeMatch = postcodePart.match(/\d{4}$/);

    if (postcodeMatch) {
      postcode = postcodeMatch[0];
    }
  }

  return {
    streetAddress,
    suburb,
    postcode,
  };
}

// Replace your existing generateCSVFile function with this.
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

// Corrected 'generateLeads' function
export async function generateLeads(
  businessType: string,
  locationInput: string,
  leadCount?: number
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

        uniqueResults = await runActorPool(
          businessType,
          structuredSuburbs,
          50,
          stateAbbr,
          leadCount
        );

        // Remove duplicates from the combined results
        uniqueResults = removeDuplicates(uniqueResults);

        // Limit the number of leads if leadCount is specified
        if (leadCount && uniqueResults.length > leadCount) {
          uniqueResults = uniqueResults.slice(0, leadCount);
        }
      } else {
        throw new Error(`Suburbs list for ${extractedLocation} not available.`);
      }
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

      const results = await scrapeGoogleMaps(businessType, locationToUse, leadCount);

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

    // Proceed with the rest of the lead generation process using the filtered savedData

    const highestRolePersons: HighestRolePerson[] = [];
    let organizationDomains: string[] = [];
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

        organizationDomains.push(websiteDomain);
        domainToCompanyIndex[websiteDomain] = index;
      }
    }

    if (organizationDomains.length > 0) {
      console.log("Processing domains with Apify:", organizationDomains);
      const contactsByDomain = await getContactsFromApify(organizationDomains);

      // For each domain's contacts, feed into GPT to find the highest role person
      const highestRolePersonsFound: HighestRolePerson[] = [];

      for (const domain of organizationDomains) {
        const contacts = contactsByDomain[domain];

        if (!contacts || contacts.length === 0) {
          console.log(`No contacts found for domain ${domain}`);
          continue;
        }

        // Prepare cleanedResults for GPT
        const cleanedResults = contacts.map((contact) => ({
          id: contact.id || '',
          name: `${contact.first_name || ''} ${contact.last_name || ''}`.trim(),
          job_title: contact.title,
        }));

        // Use GPT to find the person with the highest role
        const completion = await openai.beta.chat.completions.parse({
          model: 'gpt-4o-mini-2024-07-18',
          messages: [
            {
              role: 'system',
              content:
                'You are a helpful assistant that identifies the person with the highest role in a company based on their job title.',
            },
            {
              role: 'user',
              content: `Given the following people and their job titles: ${JSON.stringify(
                cleanedResults
              )}. Find the person with the highest role and provide their name and job title.`,
            },
          ],
          response_format: zodResponseFormat(PersonSchema, 'highest_role_person'),
        });

        const highestRolePerson = completion.choices[0].message.parsed;

        if (highestRolePerson) {
          // Find the full person details from contacts
          const personDetails = contacts.find(
            (c) =>
              `${c.first_name || ''} ${c.last_name || ''}`.trim() === highestRolePerson.name &&
              c.title === highestRolePerson.job_title
          );

          if (personDetails) {
            const companyIndex = domainToCompanyIndex[domain];

            if (companyIndex === undefined) {
              console.error(`Company index not found for domain ${domain}`);
            } else {
              highestRolePersonsFound.push({
                id: personDetails.id || '',
                first_name: personDetails.first_name || '',
                last_name: personDetails.last_name || '',
                email: personDetails.email || '',
                title: personDetails.title || '',
                linkedin_url: personDetails.linkedin_url || '',
                domain: domain,
                companyIndex: companyIndex,
              });
            }
          }
        } else {
          console.log(
            `Highest role person could not be determined for domain ${domain}.`
          );
        }
      }

      console.log("Highest role persons found:", highestRolePersonsFound);

      // Now, update savedData with highest role persons and their emails
      for (const person of highestRolePersonsFound) {
        const company = savedData[person.companyIndex];
        if (!company) {
          console.error(`Company at index ${person.companyIndex} is undefined`);
        } else {
          company.first_name = person.first_name || '';
          company.last_name = person.last_name || '';
          company.company_personal_email = person.email || '';
          company.title = person.title || '';
          company.linkedin_url = person.linkedin_url || '';
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
        }
      }
    }

    // Save the updated savedData back to finalResults.json
    saveToFile("finalResults.json", savedData);

    // --- New Step: Find company emails for companies without email ---

    // Define the interface
    interface CompanyWithoutEmail {
      index: number;
      website: string;
      domain: string;
    }

    // Identify companies without both personal and general emails and with website
    const companiesWithoutEmail: CompanyWithoutEmail[] = [];

    for (let index = 0; index < savedData.length; index++) {
      const company = savedData[index];
      if (
        (!company.company_personal_email ||
          company.company_personal_email.trim() === '') &&
        (!company.company_general_email ||
          company.company_general_email.trim() === '') &&
        company.website
      ) {
        const websiteDomain = new URL(company.website).hostname.replace(/^www\./, '');
        companiesWithoutEmail.push({
          index,
          website: company.website,
          domain: websiteDomain,
        });
      }
    }

    console.log('Companies without email to process:', companiesWithoutEmail);

    // Proceed to process companies without email if any
    if (companiesWithoutEmail.length > 0) {
      console.log('Running email scraper for companies without email...');

      const maxConcurrent = 50;
      let currentConcurrent = 0;
      let companyIdx = 0;

      await new Promise<void>((resolve, reject) => {
        function startRequest() {
          if (companyIdx >= companiesWithoutEmail.length) {
            // No more companies to process
            if (currentConcurrent === 0) {
              // All requests have finished
              resolve();
            }
            return;
          }

          if (currentConcurrent >= maxConcurrent) {
            // Wait for a request to finish before starting a new one
            return;
          }

          const company = companiesWithoutEmail[companyIdx];
          companyIdx++;

          currentConcurrent++;

          (async () => {
            try {
              // Make API call to the email scraper service
              const response = await axios.post(
                'https://emailscraperservice-54137747006.us-central1.run.app/scrape-emails',
                {
                  website: company.website,
                },
                {
                  headers: {
                    'Content-Type': 'application/json',
                  },
                }
              );

              const emails = response.data.emails;

              if (emails && emails.length > 0) {
                // Update the company in savedData
                const idx = company.index;
                const companyInSavedData = savedData[idx];
                if (
                  !companyInSavedData.company_general_email ||
                  companyInSavedData.company_general_email.trim() === ''
                ) {
                  companyInSavedData.company_general_email = emails[0]; // Take the first email
                  console.log(
                    `Added general email to company at index ${idx}: ${companyInSavedData.company_general_email}`
                  );
                }
              } else {
                console.log(`No emails found for website: ${company.website}`);
              }
            } catch (error) {
              console.error(
                `Error scraping emails for website: ${company.website}`,
                error
              );
            } finally {
              console.log(`Processed company without email: ${company.website}`);
              currentConcurrent--;

              // Start a new request if possible
              startRequest();

              // If all requests have been processed and no concurrent requests remain, resolve the promise
              if (companyIdx >= companiesWithoutEmail.length && currentConcurrent === 0) {
                resolve();
              }
            }
          })();
        }

        // Schedule initial requests with the required delays
        let totalDelay = 0;

        const scheduleNext = () => {
          if (companyIdx >= companiesWithoutEmail.length) {
            return;
          }

          if (currentConcurrent >= maxConcurrent) {
            // Wait for slots to free up
            return;
          }

          // Determine the delay
          let delayMs = 0;
          if (companyIdx <= 10) {
            delayMs = 5000; // 5 seconds between first 10 requests
          } else {
            delayMs = 1000; // 1 second between subsequent requests
          }

          totalDelay += delayMs;

          setTimeout(() => {
            startRequest();
            scheduleNext();
          }, delayMs);
        };

        // Start scheduling requests
        scheduleNext();
      });

      // Save the updated savedData back to finalResults.json
      saveToFile('finalResults.json', savedData);
    } else {
      console.log('No companies without email found.');
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

// Function to check if the location is a broad location
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
