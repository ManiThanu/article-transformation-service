import glob   from 'glob';
import Mocha  from 'mocha';
import dotenv from 'dotenv';

// Ensure Testing Env
dotenv.config({ path: 'config/.env.testing' });

// Create New Mocha Instance
const mocha = new Mocha({ reporter: 'spec' });

// Run Suite
glob('tests/**/*.js', (err, files) => {
  files.forEach(f => mocha.addFile(f));
  mocha.run(failures => process.exit(failures));
});
