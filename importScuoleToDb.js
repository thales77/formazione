const fs = require('fs');
const path = require('path');
const csvParser = require('csv-parser');
const { createClient } = require('@supabase/supabase-js');

// Supabase credentials
const supabaseUrl = 'https://pjsqwbpizlvwcqkqenkv.supabase.co'; // Replace with your Supabase URL
const supabaseKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBqc3F3YnBpemx2d2Nxa3Flbmt2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTcyNjk0MjYxOCwiZXhwIjoyMDQyNTE4NjE4fQ.JDDZG5VD09-2dZCZ4p_xy0TI23MdUrnzpTLdX0m1Wvs'; // Replace with your Supabase service role key
const supabase = createClient(supabaseUrl, supabaseKey);

// Table where the data will be inserted
const tableName = 'scuole'; // The target table

// Path to the CSV directory
const csvDirectory = './scuole'; // Directory where CSV files are stored

// Function to create the table if it doesn't already exist
const createTable = async () => {
  const query = `
    CREATE TABLE IF NOT EXISTS ${tableName} (
      annoscolastico VARCHAR(6),
      areageografica VARCHAR(20),
      regione VARCHAR(50),
      provincia VARCHAR(50),
      codiceistitutodiriferimento VARCHAR(15),
      denominazioneistitutodiriferimento VARCHAR(200),
      codicescuola VARCHAR(15),
      denominazionescuola VARCHAR(200),
      indirizzoscuola VARCHAR(200),
      capscuola VARCHAR(20),
      codicecomunescuola VARCHAR(20),
      descrizionecomune VARCHAR(200),
      descrizionecaratteristicascuola VARCHAR(50),
      descrizionetipologiagradoistruzionescuola VARCHAR(50),
      indicazionesededirettivo VARCHAR(20),
      indicazionesedeomnicomprensivo VARCHAR(20),
      indirizzoemailscuola VARCHAR(200),
      indirizzopecscuola VARCHAR(200),
      sitowebscuola VARCHAR(200),
      sedescolastica VARCHAR(20)
    );
  `;

  const { error } = await supabase.rpc('execute_sql', { sql: query });
  if (error) {
    console.error('Error creating table:', error.message);
  } else {
    console.log(`Table ${tableName} created successfully.`);
  }
};

// Function to insert data into Supabase with retry logic
const insertDataToSupabase = async (data, retries = 3) => {
  let attempts = 0;
  let success = false;

  while (attempts < retries && !success) {
    try {
      const { error } = await supabase.from(tableName).insert(data);

      if (error) {
        throw new Error(error.message);
      } else {
        success = true; // Data inserted successfully, exit loop
      }
    } catch (err) {
      attempts += 1;
      console.error(`Error inserting data (attempt ${attempts} of ${retries}): ${err.message}`);

      if (attempts < retries) {
        console.log(`Retrying in 2 seconds...`);
        await new Promise(resolve => setTimeout(resolve, 2000)); // Wait for 2 seconds before retrying
      } else {
        console.error('Max retries reached'); // Print the data causing the failure
      }
    }
  }
};

// Function to read and process a single CSV file
const readCsvAndInsertToSupabase = (csvFilePath) => {
  return new Promise((resolve, reject) => {
    const records = [];

    fs.createReadStream(csvFilePath)
      .pipe(csvParser())
      .on('data', (row) => {
        // Format the row with lowercase column names
        const formattedRow = {
          annoscolastico: row.ANNOSCOLASTICO,
          areageografica: row.AREAGEOGRAFICA,
          regione: row.REGIONE,
          provincia: row.PROVINCIA,
          codiceistitutodiriferimento: row.CODICEISTITUTORIFERIMENTO,
          denominazioneistitutodiriferimento: row.DENOMINAZIONEISTITUTORIFERIMENTO,
          codicescuola: row.CODICESCUOLA,
          denominazionescuola: row.DENOMINAZIONESCUOLA,
          indirizzoscuola: row.INDIRIZZOSCUOLA,
          capscuola: row.CAPSCUOLA,
          codicecomunescuola: row.CODICECOMUNESCUOLA,
          descrizionecomune: row.DESCRIZIONECOMUNE,
          descrizionecaratteristicascuola: row.DESCRIZIONECARATTERISTICASCUOLA,
          descrizionetipologiagradoistruzionescuola: row.DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA,
          indicazionesededirettivo: row.INDICAZIONESEDEDIRETTIVO,
          indicazionesedeomnicomprensivo: row.INDICAZIONESEDEOMNICOMPRENSIVO,
          indirizzoemailscuola: row.INDIRIZZOEMAILSCUOLA,
          indirizzopecscuola: row.INDIRIZZOPECSCUOLA,
          sitowebscuola: row.SITOWEBSCUOLA,
          sedescolastica: row.SEDESCOLASTICA
        };

        records.push(formattedRow);
      })
      .on('end', async () => {
        console.log(`Finished reading ${records.length} records from file ${csvFilePath}.`);

        // Insert the records in batches to avoid timeout issues
        const batchSize = 1000;
        for (let i = 0; i < records.length; i += batchSize) {
          const batch = records.slice(i, i + batchSize);
          await insertDataToSupabase(batch); // Retry logic built-in
        }

        console.log(`Data from ${csvFilePath} processed successfully.`);
        resolve(); // Resolve the promise after processing the file
      })
      .on('error', (err) => {
        console.error(`Error processing file ${csvFilePath}:`, err.message);
        reject(err); // Reject the promise in case of an error
      });
  });
};

// Function to process all CSV files in the directory
const processCsvFiles = async () => {
  return new Promise((resolve, reject) => {
    fs.readdir(csvDirectory, (err, files) => {
      if (err) {
        console.error('Error reading CSV directory:', err.message);
        reject(err);
        return;
      }

      const csvFiles = files.filter(file => path.extname(file).toLowerCase() === '.csv'); // Filter only CSV files

      if (csvFiles.length === 0) {
        console.log('No CSV files found in the directory.');
        resolve();
        return;
      }

      // Process each CSV file and wait for all to complete using Promise.all
      Promise.all(csvFiles.map(file => {
        const csvFilePath = path.join(csvDirectory, file);
        console.log(`Processing file: ${csvFilePath}`);
        return readCsvAndInsertToSupabase(csvFilePath);
      }))
      .then(() => {
        resolve(); // Resolve after all files have been processed
      })
      .catch((err) => {
        reject(err); // Reject if any file processing fails
      });
    });
  });
};

// Main function to orchestrate creating table and inserting data
const run = async () => {
  try {
    await createTable();       // Create the table if it doesn't exist
    await processCsvFiles();   // Process each CSV file in the directory
  } catch (err) {
    console.error('Error during execution:', err.message);
  }
};

// Start the process
run();
