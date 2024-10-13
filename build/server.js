import express from 'express';
import { generateLeads } from './generateLeads.js';
import cors from 'cors';
import path from 'path';
import fs from 'fs';
const app = express();
app.use(cors({ origin: '*' }));
app.use(express.json());
app.post('/generate-leads', async (req, res) => {
    const { businessType, location, leadCount } = req.body;
    console.log('Received request:', req.body);
    if (!businessType || !location) {
        console.log('Validation failed: Missing businessType or location');
        res.status(400).json({ error: 'businessType and location are required' });
        return;
    }
    try {
        console.log(`Starting lead generation for businessType: ${businessType}, location: ${location}, leadCount: ${leadCount}`);
        const result = await generateLeads(businessType, location, leadCount);
        if (result.error) {
            res.status(500).json({ error: result.error });
            return;
        }
        const { filename, fileSizeInBytes } = result;
        res.json({ message: 'Lead generation completed', filename, fileSizeInBytes });
    }
    catch (error) {
        console.error('Error generating leads:', error);
        res.status(500).json({ error: 'An error occurred while generating leads.' });
    }
});
// Update the download endpoint to use the filename from the query parameter
app.get('/download', (req, res) => {
    const { filename } = req.query;
    if (!filename || typeof filename !== 'string') {
        res.status(400).json({ error: 'Filename is required' });
        return;
    }
    const filepath = path.join('/tmp', 'csv_files', filename); // Use '/tmp' instead of process.cwd()
    if (fs.existsSync(filepath)) {
        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
        fs.createReadStream(filepath).pipe(res);
    }
    else {
        res.status(404).json({ error: 'File not found' });
    }
});
const PORT = process.env.PORT || 3002;
app.listen(PORT, () => {
    console.log(`Lead Generation API server is running on port ${PORT}`);
});
