const https = require('https');
const axios = require('axios');
const fs = require('fs');
const { pipeline } = require('stream');
const { promisify } = require('util');
const csvParser = require('csv-parser'); // CSV parsing library
const { Transform } = require('stream');

// Promisify pipeline for async usage
const streamPipeline = promisify(pipeline);

// Allow legacy renegotiation for older SSL/TLS versions
const agent = new https.Agent({
  minVersion: 'TLSv1.2',
  secureOptions: require('crypto').constants.SSL_OP_LEGACY_SERVER_CONNECT,
});

// Create an Axios instance with the custom HTTPS agent
const axiosInstance = axios.create({
  httpsAgent: agent,
  responseType: 'stream' // Streaming response for large files
});

// Array of URLs to download (replace .json with .csv)
const urls = [
  'https://dati.istruzione.it/opendata/opendata/catalogo/elements1/ALTABRUZZO000020240912.csv',
  'https://dati.istruzione.it/opendata/opendata/catalogo/elements1/ALTBASILICATA000020240912.csv',
  'https://dati.istruzione.it/opendata/opendata/catalogo/elements1/ALTCALABRIA000020240912.csv',
  'https://dati.istruzione.it/opendata/opendata/catalogo/elements1/ALTCAMPANIA000020240912.csv',
  'https://dati.istruzione.it/opendata/opendata/catalogo/elements1/ALTEMILIAROMAGNA000020240912.csv',
  'https://dati.istruzione.it/opendata/opendata/catalogo/elements1/ALTFRIULIVENEZIAGIULIA000020240912.csv',
  'https://dati.istruzione.it/opendata/opendata/catalogo/elements1/ALTLAZIO000020240912.csv',
  'https://dati.istruzione.it/opendata/opendata/catalogo/elements1/ALTLIGURIA000020240912.csv',
  'https://dati.istruzione.it/opendata/opendata/catalogo/elements1/ALTLOMBARDIA000020240912.csv',
  'https://dati.istruzione.it/opendata/opendata/catalogo/elements1/ALTMARCHE000020240912.csv',
  'https://dati.istruzione.it/opendata/opendata/catalogo/elements1/ALTMOLISE000020240912.csv',
  'https://dati.istruzione.it/opendata/opendata/catalogo/elements1/ALTPIEMONTE000020240912.csv',
  'https://dati.istruzione.it/opendata/opendata/catalogo/elements1/ALTPUGLIA000020240912.csv',
  'https://dati.istruzione.it/opendata/opendata/catalogo/elements1/ALTSARDEGNA000020240912.csv',
  'https://dati.istruzione.it/opendata/opendata/catalogo/elements1/ALTSICILIA000020240912.csv',
  'https://dati.istruzione.it/opendata/opendata/catalogo/elements1/ALTTOSCANA000020240912.csv',
  'https://dati.istruzione.it/opendata/opendata/catalogo/elements1/ALTTRENTINOALTOADIGE000020240912.csv',
  'https://dati.istruzione.it/opendata/opendata/catalogo/elements1/ALTUMBRIA000020240912.csv',
  'https://dati.istruzione.it/opendata/opendata/catalogo/elements1/ALTVALLEDAOSTA000020240912.csv',
  'https://dati.istruzione.it/opendata/opendata/catalogo/elements1/ALTVENETO000020240912.csv'
];

// Function to extract filename from URL
const getFileNameFromUrl = (url) => {
  return url.substring(url.lastIndexOf('/') + 1); // Get the part after the last '/'
};

// Function to download a CSV file as a stream and save it as an individual file
const downloadAndSaveCsv = async (url, filename) => {
  try {
    const response = await axiosInstance.get(url);
    const fileStream = fs.createWriteStream(filename);

    await streamPipeline(
      response.data, // Stream data from the URL
      fileStream     // Write data to individual file
    );
    console.log(`Saved data from ${url} into ${filename}`);
  } catch (error) {
    console.error(`Error downloading ${url}: ${error}`);
  }
};

// Function to merge CSV data into a single file
const mergeCsvData = async (inputFile, outputFile, isFirstFile) => {
  try {
    const fileStream = fs.createReadStream(inputFile);
    const csvStream = csvParser();
    const writeStream = fs.createWriteStream(outputFile, { flags: 'a' }); // Append to the output file

    // If it's the first file, include the header; otherwise, skip the header
    let headerWritten = !isFirstFile;

    await streamPipeline(
      fileStream,
      csvStream,
      new Transform({
        writableObjectMode: true, // We're working with parsed CSV objects
        transform(chunk, encoding, callback) {
          if (!headerWritten) {
            writeStream.write(Object.keys(chunk).join(',') + '\n'); // Write header
            headerWritten = true;
          }
          writeStream.write(Object.values(chunk).join(',') + '\n'); // Write values
          callback();
        }
      })
    );
    console.log(`Merged data from ${inputFile} into ${outputFile}`);
  } catch (error) {
    console.error(`Error merging ${inputFile}: ${error}`);
  }
};

// Main function to download and merge all CSV files, and save individual files
const downloadAndMergeAll = async () => {
  const outputFile = 'mergedData.csv';

  // Remove any existing output file
  if (fs.existsSync(outputFile)) {
    fs.unlinkSync(outputFile);
  }

  // Process each URL
  for (const [index, url] of urls.entries()) {
    const filename = getFileNameFromUrl(url); // Extract filename from URL
    console.log(`Downloading from ${url}`);

    // Download and save individual file
    await downloadAndSaveCsv(url, filename);

    // Merge the data from the individual file into the merged output
    await mergeCsvData(filename, outputFile, index === 0); // Pass isFirstFile flag for header
  }

  console.log('All data merged successfully into mergedData.csv');
};

// Start the process
downloadAndMergeAll();
