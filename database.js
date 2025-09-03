const sqlite3 = require('sqlite3').verbose();
const path = require('path');

// Create database connection
const dbPath = path.join(__dirname, 'business.db');
const db = new sqlite3.Database(dbPath);

// Initialize database tables
function initializeDatabase() {
  console.log('Initializing business database...');
  
  // Products table - enhanced from your Excel data
  db.run(`CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_id TEXT UNIQUE NOT NULL,
    serial_number TEXT,
    product_type TEXT,
    processor TEXT,
    storage TEXT,
    memory TEXT,
    display_size TEXT,
    year TEXT,
    color TEXT,
    condition TEXT,
    keyboard_layout TEXT,
    supplier_cost DECIMAL(10,2),
    additional_costs DECIMAL(10,2) DEFAULT 0,
    shopify_product_id TEXT,
    shopify_variant_id TEXT,
    is_sold INTEGER DEFAULT 0,
    date_added DATETIME DEFAULT CURRENT_TIMESTAMP,
    comments TEXT
  )`);

  // Local sales table
  db.run(`CREATE TABLE IF NOT EXISTS local_sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_id TEXT NOT NULL,
    sale_price DECIMAL(10,2) NOT NULL,
    payment_method TEXT NOT NULL, -- 'cash' or 'interac'
    customer_name TEXT,
    customer_email TEXT,
    customer_phone TEXT,
    sale_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    notes TEXT,
    FOREIGN KEY (stock_id) REFERENCES products (stock_id)
  )`);

  // Additional costs table
  db.run(`CREATE TABLE IF NOT EXISTS additional_costs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_id TEXT NOT NULL,
    cost_type TEXT NOT NULL, -- 'repair', 'charger', 'taxes', 'shipping', 'other'
    amount DECIMAL(10,2) NOT NULL,
    description TEXT,
    date_added DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stock_id) REFERENCES products (stock_id)
  )`);

  console.log('Database tables initialized successfully');
}

// Helper function to run database queries with promises
function runQuery(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function(err) {
      if (err) reject(err);
      else resolve({ id: this.lastID, changes: this.changes });
    });
  });
}

// Helper function to get data with promises
function getQuery(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) reject(err);
      else resolve(row);
    });
  });
}

// Helper function to get all data with promises
function getAllQuery(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

// Initialize database when module is loaded
initializeDatabase();

module.exports = {
  db,
  runQuery,
  getQuery,
  getAllQuery
};