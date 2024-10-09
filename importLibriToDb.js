const fs = require('fs');
const path = require('path');
const csvParser = require('csv-parser');
const { createClient } = require('@supabase/supabase-js');

// Supabase credentials
const supabaseUrl = 'https://pjsqwbpizlvwcqkqenkv.supabase.co'; // Replace with your Supabase URL
const supabaseKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBqc3F3YnBpemx2d2Nxa3Flbmt2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTcyNjk0MjYxOCwiZXhwIjoyMDQyNTE4NjE4fQ.JDDZG5VD09-2dZCZ4p_xy0TI23MdUrnzpTLdX0m1Wvs'; // Replace with your Supabase service role key
const supabase = createClient(supabaseUrl, supabaseKey);

// Table where the data will be inserted
const tableName = 'libri_scolastici'; // Replace with your table name

// Path to the CSV directory
const csvDirectory = './libri'; // Directory where CSV files are stored

// Variables to track record counts
let totalRecordsRead = 0;
let totalRecordsInserted = 0;
let failedRecords = 0;

// Function to truncate the table (delete all data but keep the structure)
const truncateTable = async () => {
  const query = `TRUNCATE TABLE ${tableName};`;

  const { error } = await supabase.rpc('execute_sql', { sql: query });
  if (error) {
    console.error('Error truncating table:', error.message);
  } else {
    console.log(`Table ${tableName} truncated successfully.`);
  }
};

// Function to insert data into Supabase with retry logic and track successful insertions
const insertDataToSupabase = async (data, retries = 3) => {
  let attempts = 0;
  let success = false;

  while (attempts < retries && !success) {
    try {
      const { error } = await supabase.from(tableName).insert(data);

      if (error) {
        throw new Error(error.message);
      } else {
        totalRecordsInserted += data.length; // Increment successful insert count
        success = true; // Data inserted successfully, exit loop
      }
    } catch (err) {
      attempts += 1;
      console.error(`Error inserting data (attempt ${attempts} of ${retries}): ${err.message}`);

      if (attempts < retries) {
        console.log(`Retrying in 2 seconds...`);
        await new Promise(resolve => setTimeout(resolve, 2000)); // Wait for 2 seconds before retrying
      } else {
        failedRecords += data.length; // Track the number of failed records
        console.error('Max retries reached. Offending data:', data); // Print the data causing the failure
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
        // Check if the prezzo field exists and handle potential empty or undefined values
        const prezzoValue = row.PREZZO
          ? parseFloat(row.PREZZO.replace(',', '.'))
          : null; // If PREZZO is empty or undefined, set it to null

        // Handle problematic ISBN values by logging if too long
        let codiceisbnValue = row.CODICEISBN;
        if (codiceisbnValue && codiceisbnValue.length > 13) {
          console.error(`Problematic ISBN: ${codiceisbnValue} is longer than 13 characters`);
        }

        // Format the row with lowercase column names
        const formattedRow = {
          codicescuola: row.CODICESCUOLA,
          annocorso: parseInt(row.ANNOCORSO, 10),
          sezioneanno: row.SEZIONEANNO,
          tipogradoscuola: row.TIPOGRADOSCUOLA,
          combinazione: row.COMBINAZIONE,
          disciplina: row.DISCIPLINA,
          codiceisbn: codiceisbnValue, // Log if too long but don't truncate
          autori: row.AUTORI,
          titolo: row.TITOLO,
          sottotitolo: row.SOTTOTITOLO,
          volume: row.VOLUME,
          editore: row.EDITORE,
          prezzo: prezzoValue, // Use the validated prezzo value
          nuovaadoz: row.NUOVAADOZ,
          daacquist: row.DAACQUIST,
          consigliato: row.CONSIGLIATO,
        };

        records.push(formattedRow);
      })
      .on('end', async () => {
        totalRecordsRead += records.length; // Track the total number of records read
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

// Function to verify the total record count in the database after insertion
const verifyRecordCount = async () => {
  const { count, error } = await supabase
    .from(tableName)
    .select('*', { count: 'exact', head: true }); // Only count the records

  if (error) {
    console.error('Error verifying record count:', error.message);
  } else {
    const recordCountInDb = count; // Get the count of records
    console.log(`Verification complete: ${recordCountInDb} records in database, ${totalRecordsRead} records read, ${totalRecordsInserted} records inserted, ${failedRecords} failed.`);
    
    if (recordCountInDb === totalRecordsInserted) {
      console.log('All records have been successfully inserted.');
    } else {
      console.warn('Mismatch in record counts. Some records may not have been inserted correctly.');
    }
  }
};

// Main function to orchestrate truncating, inserting data, and verifying
const run = async () => {
  try {
    await truncateTable();      // Truncate the table before inserting new data
    await processCsvFiles();    // Process each CSV file in the directory
    await verifyRecordCount();  // Verify record count after insertion
  } catch (err) {
    console.error('Error during execution:', err.message);
  }
};

// Start the process
run();
