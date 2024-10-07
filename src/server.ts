import express, { Request, Response } from 'express';
import { generateLeads } from './generateLeads.js';
import cors from 'cors';
import path from 'path';
import fs from 'fs';

const app = express();

app.use(cors({ origin: '*' }));
app.use(express.json());

app.post('/generate-leads', async (req: Request, res: Response): Promise<void> => {
  const { businessType, location, leadCount } = req.body;

  console.log('Received request:', req.body);

  if (!businessType || !location) {
    console.log('Validation failed: Missing businessType or location');
    res.status(400).json({ error: 'businessType and location are required' });
    return;
  }

  try {
    console.log(`Starting lead generation for businessType: ${businessType}, location: ${location}`);
    const result = await generateLeads(businessType, location, leadCount);

    if (result.error) {
      res.status(500).json({ error: result.error });
      return;
    }

    const { filename, fileSizeInBytes } = result;
    res.json({ message: 'Lead generation completed', filename, fileSizeInBytes });
  } catch (error) {
    console.error('Error generating leads:', error);
    res.status(500).json({ error: 'An error occurred while generating leads.' });
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Lead Generation API server is running on port ${PORT}`);
});
