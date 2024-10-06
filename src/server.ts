import express, { Request, Response } from 'express';
import { generateLeads } from './generateLeads.js';
import cors from 'cors'; // Import cors
import path from 'path';
import fs from 'fs';


const app = express(); // Initialize 'app' before using it

app.use(cors({
    origin: '*', // Allow all origins
}));
app.use(express.json());

// server.ts
app.post('/generate-leads', async (req: Request, res: Response): Promise<void> => {
    const { businessType, location } = req.body;
  
    console.log('Received request:', req.body); // Log request data
  
    if (!businessType || !location) {
      console.log('Validation failed: Missing businessType or location'); // Log validation failure
      res.status(400).json({ error: 'businessType and location are required' });
      return;
    }
  
    try {
      console.log(`Starting lead generation for businessType: ${businessType}, location: ${location}`); // Log before generating leads
      const result = await generateLeads(businessType, location);
  
      if (result.error) {
        // It's an error message
        res.status(500).json({ error: result.error });
        return;
      }
      
      // If we reach here, result is of type { filename: string; fileSizeInBytes: number }
      const { filename, fileSizeInBytes } = result;
      
      res.json({ message: 'Lead generation completed', filename, fileSizeInBytes });
    } catch (error) {
      console.error('Error generating leads:', error); // Log detailed error
      res.status(500).json({ error: 'An error occurred while generating leads.' });
    }
});
// make it main
// Update the download endpoint to use the filename from the query parameter
app.get('/download', (req: Request, res: Response) => {
  const { filename } = req.query;

  if (!filename || typeof filename !== 'string') {
    res.status(400).json({ error: 'Filename is required' });
    return;
  }

  const filepath = path.join(process.cwd(), "csv_files", filename);

  if (fs.existsSync(filepath)) {
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    fs.createReadStream(filepath).pipe(res);
  } else {
    res.status(404).json({ error: 'File not found' });
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Lead Generation API server is running on port ${PORT}`);
});