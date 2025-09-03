const express = require('express');
const multer = require('multer');
const XLSX = require('xlsx');
const fetch = require('node-fetch');
const cors = require('cors');
const path = require('path');
const { db, runQuery, getQuery, getAllQuery } = require('./database');

const app = express();
const PORT = process.env.PORT || 3000;

// =============================================================================
// IMAGE SCRAPER INTEGRATION CONFIGURATION
// =============================================================================
const SCRAPER_API_BASE = 'http://localhost:3001'; // Scraper runs on port 3001

// Middleware
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.static('public'));

// Configure multer for file uploads
const storage = multer.memoryStorage();
const upload = multer({ 
  storage: storage,
  limits: { fileSize: 10 * 1024 * 1024 } // 10MB limit
});

// Serve the dashboard as main page
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'dashboard.html'));
});

// Serve the Excel upload page
app.get('/excel', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// NEW ROUTES - Add these lines
app.get('/sales', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'sales.html'));
});

// API route to lookup product by stock ID
app.get('/api/product/:stockId', async (req, res) => {
  try {
    const { stockId } = req.params;
    const product = await getQuery('SELECT * FROM products WHERE stock_id = ?', [stockId]);
    
    if (product) {
      res.json({ success: true, product });
    } else {
      res.status(404).json({ error: 'Product not found' });
    }
  } catch (error) {
    console.error('Product lookup error:', error);
    res.status(500).json({ error: 'Database error' });
  }
});

// API route to record a sale
app.post('/api/record-sale', async (req, res) => {
  try {
    const { stockId, salePrice, paymentMethod, customerName, customerEmail, customerPhone, notes } = req.body;
    
    // Insert sale record
    await runQuery(`
      INSERT INTO local_sales (stock_id, sale_price, payment_method, customer_name, customer_email, customer_phone, notes)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `, [stockId, salePrice, paymentMethod, customerName, customerEmail, customerPhone, notes]);
    
    // Mark product as sold
    await runQuery('UPDATE products SET is_sold = 1 WHERE stock_id = ?', [stockId]);
    
    res.json({ success: true, message: 'Sale recorded successfully' });
  } catch (error) {
    console.error('Sale recording error:', error);
    res.status(500).json({ error: 'Failed to record sale' });
  }
});

// Serve costs page
app.get('/costs', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'costs.html'));
});

// API route to add additional cost
app.post('/api/add-cost', async (req, res) => {
  try {
    const { stockId, costType, amount, description } = req.body;
    
    // Insert cost record
    await runQuery(`
      INSERT INTO additional_costs (stock_id, cost_type, amount, description)
      VALUES (?, ?, ?, ?)
    `, [stockId, costType, amount, description]);
    
    // Calculate total additional costs for this product
    const result = await getQuery(`
      SELECT SUM(amount) as total FROM additional_costs WHERE stock_id = ?
    `, [stockId]);
    
    const totalCosts = result.total || 0;
    
    // Update product's additional_costs field
    await runQuery(`
      UPDATE products SET additional_costs = ? WHERE stock_id = ?
    `, [totalCosts, stockId]);
    
    res.json({ success: true, totalCosts: totalCosts });
  } catch (error) {
    console.error('Cost adding error:', error);
    res.status(500).json({ error: 'Failed to add cost' });
  }
});

// API route to get cost history for a product
app.get('/api/costs/:stockId', async (req, res) => {
  try {
    const { stockId } = req.params;
    const costs = await getAllQuery(`
      SELECT * FROM additional_costs WHERE stock_id = ? ORDER BY date_added DESC
    `, [stockId]);
    
    res.json({ success: true, costs });
  } catch (error) {
    console.error('Cost history error:', error);
    res.status(500).json({ error: 'Failed to get cost history' });
  }
});

// Serve reports page
app.get('/reports', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'reports.html'));
});

// API route for generating reports
app.get('/api/reports', async (req, res) => {
  try {
    const { type, startDate, endDate } = req.query;
    let result = {};

    // Build date filter for SQL queries
    let dateFilter = '';
    let dateParams = [];
    if (startDate && endDate) {
      dateFilter = ' AND date(sale_date) BETWEEN ? AND ?';
      dateParams = [startDate, endDate];
    }

    switch (type) {
      case 'interac-sales':
        // Government compliance - Interac only sales
        const interacSales = await getAllQuery(`
          SELECT ls.*, p.product_type, p.serial_number, p.processor 
          FROM local_sales ls 
          JOIN products p ON ls.stock_id = p.stock_id 
          WHERE ls.payment_method = 'interac'${dateFilter}
          ORDER BY ls.sale_date DESC
        `, dateParams);

        const interacStats = await getQuery(`
          SELECT 
            COUNT(*) as total_sales,
            SUM(sale_price) as total_amount,
            AVG(sale_price) as average_sale
          FROM local_sales 
          WHERE payment_method = 'interac'${dateFilter}
        `, dateParams);

        result = { data: interacSales, stats: interacStats };
        break;

      case 'all-sales':
        // Complete sales report (cash + interac)
        const allSales = await getAllQuery(`
          SELECT ls.*, p.product_type, p.serial_number, p.processor, p.supplier_cost, p.additional_costs,
                 (ls.sale_price - p.supplier_cost - p.additional_costs) as profit
          FROM local_sales ls 
          JOIN products p ON ls.stock_id = p.stock_id 
          WHERE 1=1${dateFilter}
          ORDER BY ls.sale_date DESC
        `, dateParams);

        const allStats = await getQuery(`
          SELECT 
            COUNT(*) as total_sales,
            SUM(sale_price) as total_revenue,
            SUM(CASE WHEN payment_method = 'cash' THEN sale_price ELSE 0 END) as cash_sales,
            SUM(CASE WHEN payment_method = 'interac' THEN sale_price ELSE 0 END) as interac_sales
          FROM local_sales ls
          WHERE 1=1${dateFilter}
        `, dateParams);

        result = { data: allSales, stats: allStats };
        break;

      case 'inventory':
        // Current inventory overview
        const inventory = await getAllQuery(`
          SELECT stock_id, product_type, processor, storage, memory, color, condition, 
                 supplier_cost, additional_costs, is_sold, date_added
          FROM products 
          ORDER BY date_added DESC
        `);

        const inventoryStats = await getQuery(`
          SELECT 
            COUNT(*) as total_products,
            COUNT(CASE WHEN is_sold = 0 THEN 1 END) as available,
            COUNT(CASE WHEN is_sold = 1 THEN 1 END) as sold,
            SUM(supplier_cost + additional_costs) as total_invested
          FROM products
        `);

        result = { data: inventory, stats: inventoryStats };
        break;

      case 'profit':
        // Profit analysis
        const profitData = await getAllQuery(`
          SELECT ls.stock_id, ls.sale_price, ls.payment_method, ls.sale_date,
                 p.product_type, p.supplier_cost, p.additional_costs,
                 (ls.sale_price - p.supplier_cost - p.additional_costs) as profit,
                 ROUND(((ls.sale_price - p.supplier_cost - p.additional_costs) / ls.sale_price * 100), 2) as profit_margin
          FROM local_sales ls 
          JOIN products p ON ls.stock_id = p.stock_id 
          WHERE 1=1${dateFilter}
          ORDER BY profit DESC
        `, dateParams);

        const profitStats = await getQuery(`
          SELECT 
            COUNT(*) as sales_count,
            SUM(ls.sale_price) as total_revenue,
            SUM(p.supplier_cost + p.additional_costs) as total_costs,
            SUM(ls.sale_price - p.supplier_cost - p.additional_costs) as total_profit,
            AVG((ls.sale_price - p.supplier_cost - p.additional_costs) / ls.sale_price * 100) as avg_profit_margin
          FROM local_sales ls 
          JOIN products p ON ls.stock_id = p.stock_id 
          WHERE 1=1${dateFilter}
        `, dateParams);

        result = { data: profitData, stats: profitStats };
        break;

      case 'costs':
        // Cost breakdown
        const costData = await getAllQuery(`
          SELECT ac.*, p.product_type, p.serial_number
          FROM additional_costs ac
          JOIN products p ON ac.stock_id = p.stock_id
          ORDER BY ac.date_added DESC
        `);

        const costStats = await getQuery(`
          SELECT 
            COUNT(*) as total_entries,
            SUM(amount) as total_costs,
            AVG(amount) as average_cost
          FROM additional_costs
        `);

        result = { data: costData, stats: costStats };
        break;

      default:
        return res.status(400).json({ error: 'Invalid report type' });
    }

    res.json(result);

  } catch (error) {
    console.error('Report generation error:', error);
    res.status(500).json({ error: 'Failed to generate report' });
  }
});

// API route for dashboard stats
app.get('/api/dashboard-stats', async (req, res) => {
  try {
    const stats = await getQuery(`
      SELECT 
        COUNT(*) as totalProducts,
        COUNT(CASE WHEN is_sold = 0 THEN 1 END) as availableProducts,
        COUNT(CASE WHEN is_sold = 1 THEN 1 END) as soldProducts
      FROM products
    `);

    const salesStats = await getQuery(`
      SELECT 
        COALESCE(SUM(sale_price), 0) as totalSales
      FROM local_sales
    `);

    res.json({
      totalProducts: stats.totalProducts || 0,
      availableProducts: stats.availableProducts || 0,
      soldProducts: stats.soldProducts || 0,
      totalSales: salesStats.totalSales || 0
    });

  } catch (error) {
    console.error('Dashboard stats error:', error);
    res.status(500).json({ error: 'Failed to load stats' });
  }
});

// =============================================================================
// NEW: IMAGE INTEGRATION ENDPOINTS
// =============================================================================

// Test connection to scraper API
app.get('/api/test-scraper-connection', async (req, res) => {
  try {
    console.log('🔌 Testing connection to scraper API...');
    
    const response = await fetch(`${SCRAPER_API_BASE}/api/health`, {
      timeout: 5000
    });
    
    if (response.ok) {
      const healthData = await response.json();
      console.log('✅ Scraper API connection successful');
      
      res.json({
        success: true,
        message: 'Successfully connected to MacBook Image Scraper API',
        scraperHealth: healthData,
        integration: 'ready'
      });
    } else {
      throw new Error(`Scraper API responded with status: ${response.status}`);
    }
    
  } catch (error) {
    console.error('❌ Scraper API connection failed:', error.message);
    
    res.json({
      success: false,
      message: 'Could not connect to MacBook Image Scraper API',
      error: error.message,
      solution: 'Make sure the scraper server is running on port 3001',
      startCommand: 'cd scraper && node server.js'
    });
  }
});

// Find images for specific product (used during Shopify sync)
app.post('/api/find-product-images', async (req, res) => {
  try {
    const { productSpecs } = req.body;
    
    console.log('🔍 Searching for images for product:', productSpecs);
    
    // Build query parameters for scraper API
    const queryParams = new URLSearchParams();
    
    if (productSpecs.productType) queryParams.append('productType', productSpecs.productType);
    if (productSpecs.displaySize) queryParams.append('displaySize', productSpecs.displaySize);
    if (productSpecs.processor) queryParams.append('processor', productSpecs.processor);
    if (productSpecs.year) queryParams.append('year', productSpecs.year);
    if (productSpecs.storage) queryParams.append('storage', productSpecs.storage);
    if (productSpecs.memory) queryParams.append('memory', productSpecs.memory);
    if (productSpecs.color) queryParams.append('color', productSpecs.color);
    if (productSpecs.condition) queryParams.append('condition', productSpecs.condition);
    if (productSpecs.keyboardLayout) queryParams.append('keyboardLayout', productSpecs.keyboardLayout);
    
    const searchUrl = `${SCRAPER_API_BASE}/api/find-images?${queryParams.toString()}`;
    console.log('🔗 Querying scraper:', searchUrl);
    
    const response = await fetch(searchUrl);
    
    if (!response.ok) {
      throw new Error(`Scraper search failed: ${response.status}`);
    }
    
    const searchResults = await response.json();
    
    console.log(`🎯 Image search results: ${searchResults.found ? `${searchResults.matchCount} matches` : 'no matches'}`);
    
    res.json({
      success: true,
      query: productSpecs,
      imageSearchResults: searchResults,
      hasImages: searchResults.found,
      bestMatch: searchResults.matches?.[0] || null
    });
    
  } catch (error) {
    console.error('❌ Product image search failed:', error.message);
    
    res.status(500).json({
      success: false,
      error: error.message,
      message: 'Failed to search for product images'
    });
  }
});

// Process Excel file endpoint
app.post('/api/process-excel', upload.single('excelFile'), async (req, res) => {
  try {
    console.log('📊 Processing Excel file with enhanced inventory tracking...');
    
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    // Parse Excel file
    const workbook = XLSX.read(req.file.buffer, { type: 'buffer' });
    console.log(`📋 Found sheets: ${workbook.SheetNames.join(', ')}`);
    
    const worksheet = workbook.Sheets[workbook.SheetNames[0]];
    
    // Parse data - try multiple methods
    let data = [];
    
    try {
      data = XLSX.utils.sheet_to_json(worksheet);
      console.log(`✅ Method 1 success: ${data.length} rows`);
    } catch (e) {
      console.log('Method 1 failed, trying method 2');
      const arrayData = XLSX.utils.sheet_to_json(worksheet, { header: 1 });
      if (arrayData.length > 1) {
        const headers = arrayData[0];
        data = arrayData.slice(1).map(row => {
          const obj = {};
          headers.forEach((header, i) => {
            obj[header] = row[i] || '';
          });
          return obj;
        });
        console.log(`✅ Method 2 success: ${data.length} rows`);
      }
    }

    if (data.length === 0) {
      return res.json({
        error: 'No data found in Excel file',
        debug: { sheets: workbook.SheetNames }
      });
    }

    // Enhanced Apple product filtering
    const appleProducts = data.filter(item => {
      if (!item || typeof item !== 'object') return false;
      
      const values = Object.values(item).join(' ').toLowerCase();
      return values.includes('apple') || 
             values.includes('macbook') || 
             values.includes('ipad') || 
             values.includes('iphone') ||
             values.includes('imac') ||
             values.includes('laptop') ||
             values.includes('tablet') ||
             values.includes('mac') ||
             values.includes('airpods');
    });

    console.log(`🍎 Found ${appleProducts.length} Apple products`);

    if (appleProducts.length === 0) {
      return res.json({
        error: 'No Apple products found',
        debug: {
          totalRows: data.length,
          sampleData: data.slice(0, 3)
        }
      });
    }

    // ENHANCED: Create product groups with image integration
const { productGroups, categories, totalItems, groupCount, totalUniqueUnits } = createProductGroups(appleProducts);

// NEW: Check for available images for each product group
console.log('🖼️ Checking for available images for each product group...');
const productGroupsWithImages = await addImageAvailabilityToGroups(productGroups);

    console.log(`✅ Created ${groupCount} product groups with ${totalUniqueUnits} total units`);

    res.json({
      success: true,
      totalItems,
      groupCount,
      totalUniqueUnits,
      categories,
      productGroups: productGroupsWithImages,  // ← CHANGED THIS LINE
      debug: {
        totalRows: data.length,
        filteredRows: appleProducts.length
      }
    });

  } catch (error) {
    console.error('❌ Processing error:', error);
    res.status(500).json({ 
      error: `Processing failed: ${error.message}`,
      type: error.constructor.name
    });
  }
});

// Helper function to add image availability to product groups
async function addImageAvailabilityToGroups(productGroups) {
  const enhancedGroups = { ...productGroups };
  
  for (const [groupKey, group] of Object.entries(enhancedGroups)) {
    try {
      console.log(`🔍 Checking images for: ${group.seoTitle}`);
      
      // Build search criteria
      const queryParams = new URLSearchParams();
      if (group.productType) queryParams.append('productType', group.productType);
      if (group.displaySize) queryParams.append('displaySize', group.displaySize);
      if (group.processor) queryParams.append('processor', group.processor);
      if (group.year) queryParams.append('year', group.year);
      
      const searchUrl = `${SCRAPER_API_BASE}/api/find-images?${queryParams.toString()}`;
      const response = await fetch(searchUrl, { timeout: 3000 });
      
      if (response.ok) {
        const imageResults = await response.json();
        
        enhancedGroups[groupKey].imageAvailability = {
          hasImages: imageResults.found,
          matchCount: imageResults.matchCount || 0,
          bestMatch: imageResults.matches?.[0] || null
        };
        
        if (imageResults.found) {
          console.log(`✅ Found ${imageResults.matchCount} image matches for ${group.seoTitle}`);
        }
      }
    } catch (error) {
      console.log(`⚠️ Image search failed for ${group.seoTitle}`);
    }
  }
  
  return enhancedGroups;
}

// ENHANCED: Advanced Shopify sync with deduplication
app.post('/api/sync-shopify', async (req, res) => {
  try {
    const { storeUrl, apiToken, productGroups } = req.body;

    if (!storeUrl || !apiToken || !productGroups) {
      return res.status(400).json({ error: 'Missing required data' });
    }

    console.log(`🛍️ Starting advanced Shopify sync for ${Object.keys(productGroups).length} product groups...`);

    const baseUrl = `https://${storeUrl}/admin/api/2023-10/`;
    const headers = {
      'Content-Type': 'application/json',
      'X-Shopify-Access-Token': apiToken
    };

    // Test connection
    console.log('🔗 Testing Shopify connection...');
    const testResponse = await fetch(`${baseUrl}shop.json`, { headers });
    if (!testResponse.ok) {
      throw new Error(`Shopify connection failed: ${testResponse.status} - Check your store URL and API token`);
    }
    console.log('✅ Shopify connection successful');

    // Get existing products for deduplication
    console.log('🔍 Fetching existing products for deduplication...');
    const existingProducts = await getAllExistingProductsAdvanced(baseUrl, headers);
    console.log(`📊 Found ${existingProducts.length} existing products in store`);

    // Setup collections
    console.log('🏗️ Setting up collections...');
    const collections = await setupCollectionsAdvanced(baseUrl, headers, productGroups);
    console.log(`📂 Managed ${Object.keys(collections).length} collections`);

    let results = {
      created: 0,
      updated: 0,
      errors: 0,
      skipped: 0,
      details: [],
      collectionsCreated: Object.keys(collections).length,
      variantsCreated: 0,
      variantsUpdated: 0,
      stockItemsProcessed: 0
    };

    // Process each product group with advanced deduplication
    for (const [groupKey, productGroup] of Object.entries(productGroups)) {
      try {
        console.log(`🔄 Processing: ${productGroup.seoTitle}`);
        
        // Check if product already exists
// Check if product already exists
        const existingProduct = findExistingProductAdvanced(existingProducts, productGroup);
        
        if (existingProduct) {
          console.log(`🔄 Product exists, attempting update: ${productGroup.seoTitle}`);
          console.log(`📋 Existing product ID: ${existingProduct.id} | Current variants: ${existingProduct.variants?.length || 0}`);
          
          try {
            const updateResult = await updateExistingProductAdvanced(
              baseUrl, headers, existingProduct, productGroup, collections
            );
            
            results.updated++;
            results.variantsUpdated += updateResult.variantsUpdated;
            results.stockItemsProcessed += updateResult.stockItemsProcessed;
            results.details.push(`✅ Updated: ${productGroup.seoTitle} (${updateResult.variantsUpdated} variants, ${updateResult.stockItemsProcessed} items)`);
            
            console.log(`✅ UPDATE SUCCESS: ${productGroup.seoTitle} | Variants processed: ${updateResult.variantsUpdated} | Stock items: ${updateResult.stockItemsProcessed}`);
            
          } catch (updateError) {
            console.log(`❌ UPDATE FAILED: ${productGroup.seoTitle}`);
            console.log(`🔍 Error details: ${updateError.message}`);
            console.log(`📊 Product info: ${JSON.stringify({
              productType: productGroup.productType,
              processor: productGroup.processor,
              storage: productGroup.storage,
              memory: productGroup.memory,
              totalUnits: productGroup.totalUnits,
              variantCount: Object.keys(productGroup.variants).length
            }, null, 2)}`);
            
            // DON'T create duplicate - just log and skip
            results.errors++;
            results.details.push(`❌ Update failed for: ${productGroup.seoTitle} - ${updateError.message} (SKIPPED to prevent duplicate)`);
            
            console.log(`⚠️ SKIPPING: ${productGroup.seoTitle} to prevent duplicate creation`);
            console.log(`📝 Please review this error and we can fix the update process`);
          }
          
        } else {
          console.log(`🆕 No existing product found, creating new: ${productGroup.seoTitle}`);
          console.log(`📊 New product details: ${JSON.stringify({
            productType: productGroup.productType,
            displaySize: productGroup.displaySize,
            processor: productGroup.processor,
            storage: productGroup.storage,
            memory: productGroup.memory,
            year: productGroup.year,
            totalUnits: productGroup.totalUnits,
            variantCount: Object.keys(productGroup.variants).length
          }, null, 2)}`);
          
          try {
            const createResult = await createNewProductAdvanced(
              baseUrl, headers, productGroup, collections
            );
            
            results.created++;
            results.variantsCreated += createResult.variantsCreated;
            results.stockItemsProcessed += createResult.stockItemsProcessed;
            results.details.push(`🆕 Created: ${productGroup.seoTitle} (${createResult.variantsCreated} variants, ${createResult.stockItemsProcessed} items)`);
            
            console.log(`✅ CREATE SUCCESS: ${productGroup.seoTitle} | Product ID: ${createResult.product.id} | Variants: ${createResult.variantsCreated} | Stock items: ${createResult.stockItemsProcessed}`);
            
          } catch (createError) {
            console.log(`❌ CREATE FAILED: ${productGroup.seoTitle}`);
            console.log(`🔍 Error details: ${createError.message}`);
            
            results.errors++;
            results.details.push(`❌ Create failed for: ${productGroup.seoTitle} - ${createError.message}`);
          }
        }

        // Rate limiting - Shopify allows 2 calls per second
        await new Promise(resolve => setTimeout(resolve, 500));

      } catch (error) {
        results.errors++;
        results.details.push(`❌ Error with ${productGroup.seoTitle}: ${error.message}`);
        console.error(`❌ Product sync error:`, error);
      }
    }

    console.log(`🎉 Sync complete! Created: ${results.created}, Updated: ${results.updated}, Errors: ${results.errors}`);

    res.json(results);

  } catch (error) {
    console.error('❌ Sync error:', error);
    res.status(500).json({ error: `Sync failed: ${error.message}` });
  }
});

// =============================================================================
// ENHANCED INVENTORY MANAGEMENT FUNCTIONS
// =============================================================================

function createProductGroups(appleProducts) {
  const productGroups = {};
  const categories = {};
  
  console.log(`🔄 Processing ${appleProducts.length} products with enhanced inventory tracking...`);
  
  appleProducts.forEach((item, index) => {
    try {
      // Extract core product information with better field mapping
      const stockId = item['Stock'] || item['stock'] || '';
      const serialNumber = item['Serial Number'] || item['serial'] || item['Serial'] || '';
      const comments = item['Comments'] || item['comments'] || '';
      
      // Debug logging for first few items
      if (index < 3) {
        console.log(`🔍 Processing item ${index + 1}:`, {
          stock: stockId,
          serial: serialNumber,
          processor: item['Processor'],
          color: item['Color'],
          condition: item['Condition']
        });
      }
      
      // Analyze product to determine type and specifications
      const productInfo = analyzeProductAdvanced(item);
      
      if (!productInfo) {
        console.log(`⚠️ Skipping unrecognized product at row ${index + 1}: ${item['Model'] || 'Unknown'}`);
        return;
      }

      // Create unique grouping key based on core specifications (not variants)
      const groupKey = createAdvancedGroupingKey(productInfo);

      // Initialize product group if it doesn't exist
      if (!productGroups[groupKey]) {
        productGroups[groupKey] = {
          // Core product information
          productType: productInfo.productType,
          displaySize: productInfo.displaySize,
          processor: productInfo.processor,
          storage: productInfo.storage,
          memory: productInfo.memory,
          year: productInfo.year,
          modelNumber: productInfo.modelNumber,
          
          // SEO and presentation - ENHANCED
          seoTitle: createAdvancedSEOTitle(productInfo),
          seoDescription: createAdvancedSEODescription(productInfo, productGroups[groupKey]?.variants || {}),
          seoHandle: createSEOOptimizedHandle(productInfo), // Add SEO handle
          productDescription: createAdvancedProductDescription(productInfo, productGroups[groupKey]?.variants || {}),
          
          // Pricing and organization - ENHANCED with retail pricing
          basePrice: calculateAdvancedPricing(productInfo),
          retailPrice: calculateRetailPrice(productInfo), // Add retail price
          collections: createAdvancedCollections(productInfo),
          tags: createAdvancedTags(productInfo),
          
          // Inventory tracking
          totalUnits: 0,
          variants: {},
          stockItems: [], // Track all individual stock items
          
          // Shopify integration
          shopifyProductId: null,
          lastUpdated: new Date().toISOString()
        };
        
        console.log(`🆕 Created new product group: ${productGroups[groupKey].seoTitle}`);
      }

      // Extract variant information with proper field mapping
      const color = cleanColor(item['Color'] || item['color'] || 'Space Gray');
      const condition = cleanCondition(item['Condition'] || item['condition'] || 'A');
      const keyboardLayout = determineKeyboardLayout(item);

      // Create comprehensive variant key
      const variantKey = `${color}-${condition}-${keyboardLayout}`;

      // Initialize variant if it doesn't exist
      if (!productGroups[groupKey].variants[variantKey]) {
        productGroups[groupKey].variants[variantKey] = {
          color: color,
          condition: condition,
          keyboardLayout: keyboardLayout,
          conditionDescription: getAdvancedConditionDescription(condition),
          quantity: 0,
          stockItems: [], // Individual items with stock numbers
          sku: createAdvancedSKU(productInfo, color, condition, keyboardLayout),
          price: calculateVariantPrice(productInfo, condition),
          compareAtPrice: calculateComparePrice(productInfo, condition)
        };
        
        console.log(`🎨 Created new variant: ${variantKey} for ${productInfo.productType}`);
      }

      // Add individual stock item to variant
      const stockItem = {
        stockId: stockId,
        serialNumber: serialNumber,
        comments: comments,
        condition: condition,
        color: color,
        keyboardLayout: keyboardLayout,
        dateAdded: new Date().toISOString(),
        originalData: item
      };

      // Add to variant and update counters
      productGroups[groupKey].variants[variantKey].stockItems.push(stockItem);
      productGroups[groupKey].variants[variantKey].quantity++;
      productGroups[groupKey].stockItems.push(stockItem);
      productGroups[groupKey].totalUnits++;

      // Count categories for statistics
      categories[productInfo.productType] = (categories[productInfo.productType] || 0) + 1;

      if (index < 5) {
        console.log(`✅ Processed: ${productInfo.productType} | Stock: ${stockId} | Variant: ${variantKey} | Total units in group: ${productGroups[groupKey].totalUnits}`);
      }

    } catch (error) {
      console.error(`❌ Error processing item ${index + 1} (Stock: ${item['Stock'] || 'N/A'}):`, error.message);
    }
  });
  
  // UPDATE SEO descriptions now that all variants are processed
  Object.entries(productGroups).forEach(([key, group]) => {
    const productInfo = {
      productType: group.productType,
      displaySize: group.displaySize,
      processor: group.processor,
      storage: group.storage,
      memory: group.memory,
      year: group.year
    };
    
    // Update SEO description with variant data
productGroups[key].seoDescription = createAdvancedSEODescription(productInfo, group.variants);
// Update product description with variant data
productGroups[key].productDescription = createAdvancedProductDescription(productInfo, group.variants);
  });

  // Debug: Log final results
  const totalUnits = Object.values(productGroups).reduce((sum, group) => sum + group.totalUnits, 0);
  console.log(`🎯 Created ${Object.keys(productGroups).length} unique product groups from ${appleProducts.length} items`);
  console.log(`📦 Total units tracked: ${totalUnits}`);
  
  debugProductGroups(productGroups);
  
  // Log some examples of what was created
  Object.entries(productGroups).slice(0, 3).forEach(([key, group]) => {
    console.log(`📋 Group: ${group.seoTitle} has ${group.totalUnits} units in ${Object.keys(group.variants).length} variants`);
  });

  return {
    productGroups,
    categories,
    totalItems: appleProducts.length,
    groupCount: Object.keys(productGroups).length,
    totalUniqueUnits: totalUnits
  };
}

// NEW FUNCTION - Add this after createProductGroups
async function saveProductsToDatabase(appleProducts) {
  console.log('Saving products to database...');
  
  for (const item of appleProducts) {
    try {
      const stockId = item['Stock'] || item['stock'] || '';
      const serialNumber = item['Serial Number'] || item['serial'] || item['Serial'] || '';
      
      if (!stockId) continue; // Skip items without stock ID
      
      // Analyze product to get structured data
      const productInfo = analyzeProductAdvanced(item);
      if (!productInfo) continue;
      
      // Extract variant information
      const color = cleanColor(item['Color'] || item['color'] || 'Space Gray');
      const condition = cleanCondition(item['Condition'] || item['condition'] || 'A');
      const keyboardLayout = determineKeyboardLayout(item);
      const comments = item['Comments'] || item['comments'] || '';
      
      // Insert or update product in database
      await runQuery(`
        INSERT OR REPLACE INTO products (
          stock_id, serial_number, product_type, processor, storage, memory, 
          display_size, year, color, condition, keyboard_layout, supplier_cost, 
          additional_costs, date_added, comments
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
      `, [
        stockId,
        serialNumber,
        productInfo.productType,
        productInfo.processor,
        productInfo.storage,
        productInfo.memory,
        productInfo.displaySize,
        productInfo.year,
        color,
        condition,
        keyboardLayout,
        0, // supplier_cost - you can add this later
        0, // additional_costs
        comments
      ]);
      
    } catch (error) {
      console.error('Error saving product:', stockId, error);
    }
  }
  
  console.log('Products saved to database');
}

// Add this debug function after createProductGroups (around line 320) to help diagnose the missing products:

function debugProductGroups(productGroups) {
  console.log('\n🔍 PRODUCT GROUP DEBUGGING INFORMATION:');
  console.log('========================================');
  
  let totalUnitsAcrossAllGroups = 0;
  const groupSummary = [];
  
  Object.entries(productGroups).forEach(([key, group]) => {
    const variantSummary = {};
    
    // Count units per variant type
    Object.entries(group.variants).forEach(([variantKey, variant]) => {
      const cleanKey = variantKey.replace(/-\d+$/, ''); // Remove any trailing numbers
      variantSummary[cleanKey] = (variantSummary[cleanKey] || 0) + variant.quantity;
    });
    
    groupSummary.push({
      title: group.seoTitle,
      totalUnits: group.totalUnits,
      variants: Object.keys(variantSummary).length,
      breakdown: variantSummary
    });
    
    totalUnitsAcrossAllGroups += group.totalUnits;
  });
  
  // Sort by total units descending
  groupSummary.sort((a, b) => b.totalUnits - a.totalUnits);
  
  console.log(`\n📊 SUMMARY: ${groupSummary.length} product groups with ${totalUnitsAcrossAllGroups} total units\n`);
  
  groupSummary.forEach((group, index) => {
    console.log(`${index + 1}. ${group.title}`);
    console.log(`   Units: ${group.totalUnits} | Variants: ${group.variants}`);
    Object.entries(group.breakdown).forEach(([variant, count]) => {
      console.log(`   - ${variant}: ${count} units`);
    });
    console.log('');
  });
  
  if (totalUnitsAcrossAllGroups !== 72) {
    console.log(`⚠️ WARNING: Expected 72 units but found ${totalUnitsAcrossAllGroups}`);
    console.log('Check for missing or duplicate products!\n');
  }
  
  return totalUnitsAcrossAllGroups;
}



// CORE ANALYSIS FUNCTION - Determines product type and specifications
function analyzeProductAdvanced(item) {
  // Get all relevant fields for analysis
  const model = (item['Model'] || '').toString().trim();
  const category = (item['Sub-Category'] || item['Category'] || '').toString().trim();
  const processor = (item['Processor'] || '').toString().trim();
  const brand = (item['Brand'] || '').toString().trim();
  const storage = (item['Storage'] || '').toString().trim();
  const memory = (item['Memory'] || '').toString().trim();

  // Combine all text for comprehensive analysis
  const combinedText = `${model} ${category} ${processor} ${brand}`.toLowerCase();

  console.log(`🔍 Analyzing: "${model}" | Category: "${category}" | Processor: "${processor}"`);

  // MacBook Pro Detection (Enhanced)
  if (combinedText.includes('macbook pro') || 
      (combinedText.includes('laptop') && combinedText.includes('pro')) ||
      (category.toLowerCase().includes('laptop') && processor.toLowerCase().includes('pro'))) {
    
    return {
      productType: 'MacBook Pro',
      displaySize: extractDisplaySize(model, processor) || determineDefaultSize('MacBook Pro', processor),
      processor: extractProcessorDetails(processor),
      storage: standardizeStorage(storage),
      memory: standardizeMemory(memory),
      year: extractYear(processor, model) || estimateYear(processor),
      modelNumber: extractModelNumber(processor, model),
      category: 'Laptops',
      deviceFamily: 'Mac'
    };
  }

  // MacBook Air Detection (Enhanced)
  if (combinedText.includes('macbook air') || 
      (combinedText.includes('macbook') && combinedText.includes('air')) ||
      category.toLowerCase().includes('macbook air')) {
    
    return {
      productType: 'MacBook Air',
      displaySize: extractDisplaySize(model, processor) || determineDefaultSize('MacBook Air', processor),
      processor: extractProcessorDetails(processor),
      storage: standardizeStorage(storage),
      memory: standardizeMemory(memory),
      year: extractYear(processor, model) || estimateYear(processor),
      modelNumber: extractModelNumber(processor, model),
      category: 'Laptops',
      deviceFamily: 'Mac'
    };
  }

  // Generic MacBook Detection
  if (combinedText.includes('macbook') || 
      (category.toLowerCase().includes('laptop') && brand.toLowerCase().includes('apple'))) {
    
    return {
      productType: 'MacBook',
      displaySize: extractDisplaySize(model, processor) || '13"',
      processor: extractProcessorDetails(processor),
      storage: standardizeStorage(storage),
      memory: standardizeMemory(memory),
      year: extractYear(processor, model) || estimateYear(processor),
      modelNumber: extractModelNumber(processor, model),
      category: 'Laptops',
      deviceFamily: 'Mac'
    };
  }

  // iPad Detection (Enhanced)
  if (combinedText.includes('ipad') || category.toLowerCase().includes('tablet')) {
    const ipadType = determineIPadTypeAdvanced(model, processor, category);
    
    return {
      productType: ipadType,
      displaySize: extractIPadSize(model, processor),
      processor: extractProcessorDetails(processor),
      storage: standardizeStorage(storage),
      memory: standardizeMemory(memory),
      year: extractYear(processor, model) || estimateYear(processor),
      modelNumber: extractModelNumber(processor, model),
      category: 'Tablets',
      deviceFamily: 'iPad'
    };
  }

  // iPhone Detection (Enhanced)
  if (combinedText.includes('iphone') || category.toLowerCase().includes('phone')) {
    const iphoneModel = determineIPhoneModelAdvanced(model, processor);
    
    return {
      productType: iphoneModel,
      displaySize: getIPhoneDisplaySize(iphoneModel),
      processor: extractProcessorDetails(processor),
      storage: standardizeStorage(storage),
      memory: '', // iPhones don't typically show RAM
      year: extractYear(processor, model) || estimateIPhoneYear(iphoneModel),
      modelNumber: extractModelNumber(processor, model),
      category: 'Phones',
      deviceFamily: 'iPhone'
    };
  }

  // iMac Detection (Enhanced)
  if (combinedText.includes('imac') || 
      (category.toLowerCase().includes('desktop') && combinedText.includes('imac'))) {
    
    return {
      productType: 'iMac',
      displaySize: extractDisplaySize(model, processor) || '24"',
      processor: extractProcessorDetails(processor),
      storage: standardizeStorage(storage),
      memory: standardizeMemory(memory),
      year: extractYear(processor, model) || estimateYear(processor),
      modelNumber: extractModelNumber(processor, model),
      category: 'Desktops',
      deviceFamily: 'Mac'
    };
  }

  // AirPods and Accessories Detection (Enhanced)
  if (combinedText.includes('airpods') || combinedText.includes('airpod') || 
      category.toLowerCase().includes('accessories') ||
      combinedText.includes('magic') || combinedText.includes('adapter') || 
      combinedText.includes('cable')) {
    
    let accessoryType = 'Apple Accessory';
    if (combinedText.includes('airpods')) accessoryType = 'AirPods';
    else if (combinedText.includes('magic mouse')) accessoryType = 'Magic Mouse';
    else if (combinedText.includes('magic keyboard')) accessoryType = 'Magic Keyboard';
    
    return {
      productType: accessoryType,
      displaySize: '',
      processor: '',
      storage: '',
      memory: '',
      year: extractYear(processor, model) || '2022',
      modelNumber: extractModelNumber(processor, model),
      category: 'Accessories',
      deviceFamily: 'Apple Accessory'
    };
  }

  console.log(`⚠️ Could not categorize product: "${model}" in category "${category}"`);
  return null;
}

// HELPER FUNCTIONS FOR PRODUCT ANALYSIS
function extractProcessorDetails(processor) {
  if (!processor) return 'Unknown';
  
  // Enhanced processor detection
  if (processor.includes('M3 Max')) return 'M3 Max';
  if (processor.includes('M3 Pro')) return 'M3 Pro';
  if (processor.includes('M3')) return 'M3';
  if (processor.includes('M2 Ultra')) return 'M2 Ultra';
  if (processor.includes('M2 Max')) return 'M2 Max';
  if (processor.includes('M2 Pro')) return 'M2 Pro';
  if (processor.includes('M2')) return 'M2';
  if (processor.includes('M1 Ultra')) return 'M1 Ultra';
  if (processor.includes('M1 Max')) return 'M1 Max';
  if (processor.includes('M1 Pro')) return 'M1 Pro';
  if (processor.includes('M1')) return 'M1';
  if (processor.includes('Intel')) {
    // Try to extract Intel model
    const intelMatch = processor.match(/Intel.*?(Core.*?|Xeon.*?|Celeron.*?)\s*(\w+)/i);
    if (intelMatch) return intelMatch[1] + ' ' + intelMatch[2];
    return 'Intel';
  }
  
  return processor.trim();
}

function standardizeStorage(storage) {
  if (!storage) return '';
  
  const storageUpper = storage.toUpperCase().replace(/[^0-9TBGB]/g, '');
  const match = storageUpper.match(/(\d+)(TB|GB|T|G)?/);
  
  if (match) {
    let amount = parseInt(match[1]);
    let unit = match[2] || '';
    
    // Standardize units
    if (unit === 'T') unit = 'TB';
    if (unit === 'G') unit = 'GB';
    if (!unit) {
      // Auto-detect unit based on size
      unit = amount >= 1000 ? 'TB' : 'GB';
      if (unit === 'TB') amount = amount / 1000; // Convert if needed
    }
    
    return `${amount}${unit}`;
  }
  
  return storage;
}

function standardizeMemory(memory) {
  if (!memory) return '';
  
  const memoryUpper = memory.toUpperCase().replace(/[^0-9GB]/g, '');
  const match = memoryUpper.match(/(\d+)(GB|G)?/);
  
  if (match) {
    return `${match[1]}GB`;
  }
  
  return memory;
}

function extractDisplaySize(model, processor) {
  // COMPLETE model number to screen size mapping (2016+)
  const modelSizeMap = {
    // ============ MacBook Pro ============
    // 16" MacBook Pro (2019-2025)
    'A2141': '16"',  // MacBook Pro 16" Intel 2019-2020
    'A2485': '16"',  // MacBook Pro 16" M1 Max/Pro 2021
    'A2780': '16"',  // MacBook Pro 16" M2 Pro/Max 2023
    'A2991': '16"',  // MacBook Pro 16" M3 Pro/Max 2023
    'A3112': '16"',  // MacBook Pro 16" M4 Pro/Max 2024-2025
    'A3185': '16"',  // MacBook Pro 16" M4 Pro/Max 2024-2025
    'A3401': '16"',  // MacBook Pro 16" M4 Pro/Max 2024-2025
    
    // 15" MacBook Pro (Legacy Intel, discontinued 2019)
    'A1707': '15"',  // MacBook Pro 15" Intel 2016-2017
    'A1990': '15"',  // MacBook Pro 15" Intel 2018-2019
    
    // 14" MacBook Pro (2021+)
    'A2442': '14"',  // MacBook Pro 14" M1 Pro/Max 2021
    'A2779': '14"',  // MacBook Pro 14" M2 Pro/Max 2023
    'A2992': '14"',  // MacBook Pro 14" M3 Pro/Max 2023
    'A3114': '14"',  // MacBook Pro 14" M4 Pro/Max 2024-2025
    
    // 13" MacBook Pro (2016-2022)
    'A1706': '13"',  // MacBook Pro 13" Intel Touch Bar 2016-2017
    'A1708': '13"',  // MacBook Pro 13" Intel No Touch Bar 2016-2017
    'A1989': '13"',  // MacBook Pro 13" Intel 2018
    'A2159': '13"',  // MacBook Pro 13" Intel 2019
    'A2251': '13"',  // MacBook Pro 13" Intel 2020
    'A2289': '13"',  // MacBook Pro 13" M1 2020
    'A2338': '13"',  // MacBook Pro 13" M2 2022
    
    // ============ MacBook Air ============
    // 15" MacBook Air (2023+)
    'A2941': '15"',  // MacBook Air 15" M2 2023
    'A3114': '15"',  // MacBook Air 15" M3 2024
    'A3241': '15"',  // MacBook Air 15" M4 2025
    
    // 13" MacBook Air (2018+)
    'A1932': '13"',  // MacBook Air 13" Intel 2018-2020
    'A2337': '13"',  // MacBook Air 13" M1 2020-2021
    'A2681': '13"',  // MacBook Air 13" M2 2022
    'A3113': '13"',  // MacBook Air 13" M3 2024
    'A3240': '13"',  // MacBook Air 13" M4 2025
    
    // ============ iMac ============
    // 24" iMac (Apple Silicon 2021+)
    'A2438': '24"',  // iMac 24" M1 2021 (2-port)
    'A2439': '24"',  // iMac 24" M1 2021 (4-port)
    'A2873': '24"',  // iMac 24" M3 2023 (2-port)
    'A2874': '24"',  // iMac 24" M3 2023 (4-port)
    'A3115': '24"',  // iMac 24" M4 2024 (estimated)
    
    // 27" iMac (Intel, discontinued 2022)
    'A2115': '27"',  // iMac 27" Intel Retina 5K 2019-2020
    'A1419': '27"',  // iMac 27" Intel 2012-2017
    
    // 21.5" iMac (Intel, discontinued)
    'A2116': '21.5"', // iMac 21.5" Intel 2019
    'A1418': '21.5"', // iMac 21.5" Intel 2012-2017
    
    // ============ iPad (for future compatibility) ============
    // iPad Pro 12.9"
    'A1584': '12.9"', // iPad Pro 12.9" 1st gen 2015
    'A1652': '12.9"', // iPad Pro 12.9" 1st gen 2015 (Cellular)
    'A1670': '12.9"', // iPad Pro 12.9" 2nd gen 2017
    'A1671': '12.9"', // iPad Pro 12.9" 2nd gen 2017 (Cellular)
    'A1876': '12.9"', // iPad Pro 12.9" 3rd gen 2018
    'A2014': '12.9"', // iPad Pro 12.9" 3rd gen 2018 (Cellular)
    'A1895': '12.9"', // iPad Pro 12.9" 3rd gen 2018 (1TB)
    'A2229': '12.9"', // iPad Pro 12.9" 4th gen 2020
    'A2069': '12.9"', // iPad Pro 12.9" 4th gen 2020 (Cellular)
    'A2232': '12.9"', // iPad Pro 12.9" 4th gen 2020 (1TB)
    'A2378': '12.9"', // iPad Pro 12.9" 5th gen 2021 (M1)
    'A2461': '12.9"', // iPad Pro 12.9" 5th gen 2021 (M1 Cellular)
    'A2379': '12.9"', // iPad Pro 12.9" 5th gen 2021 (M1 1TB)
    'A2436': '12.9"', // iPad Pro 12.9" 6th gen 2022 (M2)
    'A2764': '12.9"', // iPad Pro 12.9" 6th gen 2022 (M2 Cellular)
    'A2437': '12.9"', // iPad Pro 12.9" 6th gen 2022 (M2 1TB)
    
    // iPad Pro 11"
    'A1980': '11"',   // iPad Pro 11" 1st gen 2018
    'A2013': '11"',   // iPad Pro 11" 1st gen 2018 (Cellular)
    'A1934': '11"',   // iPad Pro 11" 1st gen 2018 (1TB)
    'A2228': '11"',   // iPad Pro 11" 2nd gen 2020
    'A2068': '11"',   // iPad Pro 11" 2nd gen 2020 (Cellular)
    'A2230': '11"',   // iPad Pro 11" 2nd gen 2020 (1TB)
    'A2377': '11"',   // iPad Pro 11" 3rd gen 2021 (M1)
    'A2459': '11"',   // iPad Pro 11" 3rd gen 2021 (M1 Cellular)
    'A2301': '11"',   // iPad Pro 11" 3rd gen 2021 (M1 1TB)
    'A2435': '11"',   // iPad Pro 11" 4th gen 2022 (M2)
    'A2761': '11"',   // iPad Pro 11" 4th gen 2022 (M2 Cellular)
    'A2302': '11"',   // iPad Pro 11" 4th gen 2022 (M2 1TB)
    
    // iPad Pro 10.5" (Legacy)
    'A1701': '10.5"', // iPad Pro 10.5" 2017
    'A1709': '10.5"', // iPad Pro 10.5" 2017 (Cellular)
    
    // iPad Pro 9.7" (Legacy)
    'A1673': '9.7"',  // iPad Pro 9.7" 2016
    'A1674': '9.7"',  // iPad Pro 9.7" 2016 (Cellular)
    'A1675': '9.7"',  // iPad Pro 9.7" 2016 (128GB)
    
    // iPad Air (10.9")
    'A2316': '10.9"', // iPad Air 4th gen 2020
    'A2324': '10.9"', // iPad Air 4th gen 2020 (Cellular)
    'A2325': '10.9"', // iPad Air 4th gen 2020 (256GB)
    'A2588': '10.9"', // iPad Air 5th gen 2022 (M1)
    'A2589': '10.9"', // iPad Air 5th gen 2022 (M1 Cellular)
    'A2591': '10.9"', // iPad Air 5th gen 2022 (M1 256GB)
    
    // iPad (10.2")
    'A2197': '10.2"', // iPad 7th gen 2019
    'A2200': '10.2"', // iPad 7th gen 2019 (Cellular)
    'A2198': '10.2"', // iPad 7th gen 2019 (128GB)
    'A2270': '10.2"', // iPad 8th gen 2020
    'A2428': '10.2"', // iPad 8th gen 2020 (Cellular)
    'A2429': '10.2"', // iPad 8th gen 2020 (128GB)
    'A2602': '10.2"', // iPad 9th gen 2021
    'A2603': '10.2"', // iPad 9th gen 2021 (Cellular)
    'A2604': '10.2"', // iPad 9th gen 2021 (256GB)
    
    // iPad Mini (8.3")
    'A2568': '8.3"',  // iPad Mini 6th gen 2021
    'A2569': '8.3"',  // iPad Mini 6th gen 2021 (Cellular)
    'A2567': '8.3"'   // iPad Mini 6th gen 2021 (256GB)
  };
  
  // First try exact model number mapping
  if (model && modelSizeMap[model]) {
    return modelSizeMap[model];
  }
  
  // Fallback: extract from processor description
  const sizeMatches = [
    { pattern: /27["\s]?(?:inch)?/i, size: '27"' },
    { pattern: /24["\s]?(?:inch)?/i, size: '24"' },
    { pattern: /21\.?5["\s]?(?:inch)?/i, size: '21.5"' },
    { pattern: /16["\s]?(?:inch)?/i, size: '16"' },
    { pattern: /15["\s]?(?:inch)?/i, size: '15"' },
    { pattern: /14["\s]?(?:inch)?/i, size: '14"' },
    { pattern: /13["\s]?(?:inch)?/i, size: '13"' },
    { pattern: /12\.?9["\s]?(?:inch)?/i, size: '12.9"' },
    { pattern: /11["\s]?(?:inch)?/i, size: '11"' },
    { pattern: /10\.?9["\s]?(?:inch)?/i, size: '10.9"' },
    { pattern: /10\.?5["\s]?(?:inch)?/i, size: '10.5"' },
    { pattern: /10\.?2["\s]?(?:inch)?/i, size: '10.2"' },
    { pattern: /9\.?7["\s]?(?:inch)?/i, size: '9.7"' },
    { pattern: /8\.?3["\s]?(?:inch)?/i, size: '8.3"' }
  ];
  
  for (const match of sizeMatches) {
    if (match.pattern.test(processor)) {
      return match.size;
    }
  }
  
  // Smart defaults based on device type and processor
  const processorLower = processor.toLowerCase();
  
  // iMac defaults
  if (processorLower.includes('imac')) {
    if (processorLower.includes('m1') || processorLower.includes('m3') || processorLower.includes('m4')) {
      return '24"'; // Apple Silicon iMacs are 24"
    }
    return '27"'; // Intel iMacs default to 27"
  }
  
  // MacBook defaults
  if (processorLower.includes('max')) return '16"'; // M1/M2/M3 Max typically 16"
  if (processorLower.includes('pro') && !processorLower.includes('air')) return '14"'; // M1/M2/M3 Pro typically 14"
  if (processorLower.includes('air')) return '13"'; // MacBook Air default
  
  // iPad defaults
  if (processorLower.includes('ipad')) {
    if (processorLower.includes('pro')) return '11"'; // iPad Pro default
    if (processorLower.includes('air')) return '10.9"'; // iPad Air
    if (processorLower.includes('mini')) return '8.3"'; // iPad Mini
    return '10.2"'; // Regular iPad default
  }
  
  return '13"'; // Safe default for unknown devices
}

function extractYear(processor, model) {
  const combined = `${processor} ${model}`;
  const match = combined.match(/(20\d{2})/);
  return match ? match[1] : '';
}

function extractModelNumber(processor, model) {
  // Try to extract model numbers from processor or model fields
  const combined = `${processor} ${model}`;
  const match = combined.match(/([A-Z]\d{4}[A-Z]*)/);
  return match ? match[1] : '';
}

function estimateYear(processor) {
  // Estimate year based on processor
  if (processor.includes('M3')) return '2023';
  if (processor.includes('M2')) return '2022';
  if (processor.includes('M1')) return '2020';
  if (processor.includes('Intel')) return '2019';
  
  return '2022'; // Default recent year
}

function determineDefaultSize(productType, processor) {
  // Use processor info to determine likely screen size
  if (processor.includes('16')) return '16"';
  if (processor.includes('15')) return '15"';
  if (processor.includes('14')) return '14"';
  if (processor.includes('13')) return '13"';
  
  // Default sizes by product type
  const defaultSizes = {
    'MacBook Pro': '16"',
    'MacBook Air': '13"',
    'MacBook': '13"',
    'iMac': '24"'
  };
  
  return defaultSizes[productType] || '';
}

function determineIPadTypeAdvanced(model, processor, category) {
  const modelLower = model.toLowerCase();
  
  if (modelLower.includes('ipad pro')) return 'iPad Pro';
  if (modelLower.includes('ipad air')) return 'iPad Air';
  if (modelLower.includes('ipad mini')) return 'iPad Mini';
  if (modelLower.includes('ipad')) return 'iPad';
  
  return 'iPad';
}

function determineIPhoneModelAdvanced(model, processor) {
  const modelLower = model.toLowerCase();
  
  if (modelLower.includes('iphone 15')) return 'iPhone 15';
  if (modelLower.includes('iphone 14')) return 'iPhone 14';
  if (modelLower.includes('iphone 13')) return 'iPhone 13';
  if (modelLower.includes('iphone 12')) return 'iPhone 12';
  if (modelLower.includes('iphone 11')) return 'iPhone 11';
  if (modelLower.includes('iphone se')) return 'iPhone SE';
  
  return 'iPhone';
}

function extractIPadSize(model, processor) {
  const combined = `${model} ${processor}`;
  const match = combined.match(/(\d+(?:\.\d+)?)["\s]?(?:inch)?/i);
  return match ? `${match[1]}"` : '10.9"'; // Default iPad size
}

function getIPhoneDisplaySize(iphoneModel) {
  const sizes = {
    'iPhone 15': '6.1"',
    'iPhone 14': '6.1"',
    'iPhone 13': '6.1"',
    'iPhone 12': '6.1"',
    'iPhone 11': '6.1"',
    'iPhone SE': '4.7"'
  };
  
  return sizes[iphoneModel] || '6.1"';
}

function estimateIPhoneYear(iphoneModel) {
  const years = {
    'iPhone 15': '2023',
    'iPhone 14': '2022',
    'iPhone 13': '2021',
    'iPhone 12': '2020',
    'iPhone 11': '2019',
    'iPhone SE': '2022'
  };
  
  return years[iphoneModel] || '2022';
}

// PRODUCT GROUPING AND ORGANIZATION FUNCTIONS
function createAdvancedGroupingKey(productInfo) {
  // Create a unique key that groups products by core specs (not variants)
  return [
    productInfo.productType,
    productInfo.displaySize,
    productInfo.processor,
    productInfo.storage,
    productInfo.memory,
    productInfo.year
  ].filter(Boolean).join('_').replace(/[^a-zA-Z0-9_]/g, '');
}

function createAdvancedSEOTitle(productInfo) {
  // SEO-optimized title starting with "Refurbished" for ranking
  let title = 'Refurbished '; // Start with primary keyword
  
  // Add product type
  title += productInfo.productType;
  
  // Add display size
  if (productInfo.displaySize) title += ` ${productInfo.displaySize}`;
  
  // Add processor (clean format)
  if (productInfo.processor) {
    const cleanProcessor = productInfo.processor
      .replace('Apple ', '')
      .replace(' chip', '')
      .replace(' Chip', '');
    title += ` ${cleanProcessor}`;
  }
  
  // Add year
  if (productInfo.year) title += ` ${productInfo.year}`;
  
  // Add specs
  const specs = [];
  if (productInfo.storage) specs.push(productInfo.storage);
  if (productInfo.memory) specs.push(productInfo.memory);
  
  if (specs.length > 0) {
    title += ` ${specs.join(' ')}`;
  }
  
  // Keep under 60 characters for SEO
  return title.length > 60 ? title.substring(0, 57) + '...' : title;
}

function createAdvancedSEODescription(productInfo, variants) {
  // Determine the primary grade for this product (most common grade)
  let primaryGrade = 'A';
  if (variants && Object.keys(variants).length > 0) {
    const gradeCount = {};
    Object.values(variants).forEach(variant => {
      const grade = variant.condition;
      gradeCount[grade] = (gradeCount[grade] || 0) + variant.quantity;
    });
    // Get the grade with most units
    primaryGrade = Object.keys(gradeCount).reduce((a, b) => 
      gradeCount[a] > gradeCount[b] ? a : b
    );
  }
  
  const retailPrice = calculateRetailPrice(productInfo);
  const discounts = { 'A': 30, 'B': 32, 'C': 34, 'D': 39 };
  const discount = discounts[primaryGrade];
  
  let description = '';
  const productName = productInfo.productType + (productInfo.displaySize ? ` ${productInfo.displaySize}` : '');
  const processorShort = productInfo.processor?.replace('Apple ', '').replace(' chip', '') || 'M2';
  
  // Grade-specific descriptions following your examples
  switch (primaryGrade) {
    case 'A':
      description = `Certified refurbished ${productName} ${processorShort} like-new condition. Save ${discount}% vs retail. 90-day warranty, free North American shipping. Shop premium quality!`;
      break;
    case 'B':
      description = `Refurbished ${productName} ${processorShort} excellent condition. Save ${discount}% vs retail. 90-day warranty, free shipping across North America. Great value today!`;
      break;
    case 'C':
      description = `Save ${discount}% on refurbished ${productName} ${processorShort}. Fully tested, 90-day warranty, free North American shipping. Budget-friendly Apple quality - order now!`;
      break;
    case 'D':
      description = `Maximum savings! ${productName} ${processorShort} refurbished - save ${discount}% vs retail. 90-day warranty included. Best value Apple quality - shop now!`;
      break;
    default:
      description = `Certified refurbished ${productName} ${processorShort}. Save up to 39% vs retail. 90-day warranty, free North American shipping. Shop quality Apple devices!`;
  }
  
  // Ensure it's within 150-160 characters for SEO
  if (description.length > 160) {
    description = description.substring(0, 157) + '...';
  }
  
  return description;
}

function calculateRetailPrice(productInfo) {
  // Calculate estimated retail prices for discount calculations
  const retailPrices = {
    'MacBook Pro': 2799,
    'MacBook Air': 1599,
    'MacBook': 1999,
    'iPad Pro': 1399,
    'iPad Air': 899,
    'iPad': 579,
    'iPad Mini': 699,
    'iPhone 15': 1129,
    'iPhone 14': 999,
    'iPhone 13': 849,
    'iPhone 12': 699,
    'iPhone 11': 579,
    'iPhone SE': 579,
    'iPhone': 849,
    'iMac': 1799,
    'Mac Studio': 2799,
    'Mac Mini': 899,
    'AirPods': 229,
    'Magic Mouse': 129,
    'Magic Keyboard': 229,
    'Apple Accessory': 129
  };
  
  let basePrice = retailPrices[productInfo.productType] || 999;
  
  // Adjust for specifications
  if (productInfo.storage) {
    const storageNum = parseInt(productInfo.storage);
    if (storageNum >= 2000) basePrice += 1000; // 2TB+
    else if (storageNum >= 1000) basePrice += 500; // 1TB+
    else if (storageNum >= 512) basePrice += 300; // 512GB
  }
  
  if (productInfo.memory) {
    const memoryNum = parseInt(productInfo.memory);
    if (memoryNum >= 64) basePrice += 1400; // 64GB+
    else if (memoryNum >= 32) basePrice += 800; // 32GB+
    else if (memoryNum >= 16) basePrice += 500; // 16GB
  }
  
  // Processor adjustments
  if (productInfo.processor?.includes('Max')) basePrice += 1000;
  else if (productInfo.processor?.includes('Pro')) basePrice += 500;
  else if (productInfo.processor?.includes('Ultra')) basePrice += 1500;
  
  // Display size adjustments
  if (productInfo.displaySize) {
    const size = parseFloat(productInfo.displaySize);
    if (size >= 16) basePrice += 300;
    else if (size >= 15) basePrice += 200;
  }
  
  return Math.round(basePrice);
}

function createAdvancedProductDescription(productInfo, variants) {
  // Determine keyboard availability from variants
  let keyboardInfo = '';
  
  if (variants && Object.keys(variants).length > 0) {
    const hasEnglish = Object.values(variants).some(v => v.keyboardLayout === 'English');
    const hasFrench = Object.values(variants).some(v => v.keyboardLayout === 'French Canadian');
    
    if (hasEnglish && hasFrench) {
      keyboardInfo = 'Available in English and French Canadian keyboards';
    } else if (hasFrench) {
      keyboardInfo = 'French Canadian keyboard';
    } else {
      keyboardInfo = 'English keyboard';
    }
  } else {
    keyboardInfo = 'English keyboard';
  }

  // Get actual prices from variants for dynamic pricing
  const actualPrices = {};
  if (variants && Object.keys(variants).length > 0) {
    Object.values(variants).forEach(variant => {
      const grade = variant.condition;
      const price = parseFloat(variant.price) || 0;
      if (!actualPrices[grade] || price < actualPrices[grade]) {
        actualPrices[grade] = price;
      }
    });
  }

  // Calculate dynamic pricing
  const gradeAPrice = actualPrices['A'] || calculateVariantPrice(productInfo, 'A');
  const gradeBPrice = actualPrices['B'] || calculateVariantPrice(productInfo, 'B');
  const gradeCPrice = actualPrices['C'] || calculateVariantPrice(productInfo, 'C');
  const gradeDPrice = actualPrices['D'] || calculateVariantPrice(productInfo, 'D');
  
  // Calculate retail from Grade A
  const dynamicRetailPrice = Math.round(gradeAPrice / 0.70);
  
  // Find lowest available price for hero
  const availablePrices = Object.values(actualPrices).filter(p => p > 0);
  const lowestPrice = availablePrices.length > 0 ? Math.min(...availablePrices) : gradeDPrice;

  // Determine primary grade (most common)
  let primaryGrade = 'A';
  let primaryPrice = gradeAPrice;
  
  if (variants && Object.keys(variants).length > 0) {
    const gradeCount = {};
    Object.values(variants).forEach(variant => {
      const grade = variant.condition;
      gradeCount[grade] = (gradeCount[grade] || 0) + variant.quantity;
    });
    
    primaryGrade = Object.keys(gradeCount).reduce((a, b) => 
      gradeCount[a] > gradeCount[b] ? a : b
    );
    
    const priceMap = { 'A': gradeAPrice, 'B': gradeBPrice, 'C': gradeCPrice, 'D': gradeDPrice };
    primaryPrice = priceMap[primaryGrade];
  }

  // Determine target audience
  let audienceAndUse = '';
  if (productInfo.productType.includes('Pro')) {
    audienceAndUse = 'Perfect for creative professionals, developers, and power users who demand peak performance for video editing, 3D rendering, and intensive multitasking.';
  } else if (productInfo.productType.includes('Air')) {
    audienceAndUse = 'Ideal for students, professionals, and everyday users who need portability without sacrificing performance for work, study, and creative projects.';
  } else {
    audienceAndUse = 'Great for professionals and enthusiasts who need reliable Apple performance for work, creativity, and daily computing tasks.';
  }

  // Random FAQ selection
  const faqPool = [
    {q: "Is this genuine Apple hardware?", a: "100% authentic Apple hardware - never refurbished knockoffs or third-party parts."},
    {q: "What does Grade A, B, C, D mean?", a: "Grade A=like-new (30% off), Grade B=excellent (32% off), Grade C=good (34% off), Grade D=fair (39% off retail)."},
    {q: "Do I get a warranty?", a: "Yes! Every MacBook includes our 90-day warranty plus optional extended coverage available."},
    {q: "Can I return if not satisfied?", a: "Absolutely! 30-day no-questions-asked return policy for your peace of mind."},
    {q: "What's included in the box?", a: "Your MacBook, original Apple charger, and all necessary documentation."},
    {q: "How long will this last?", a: "Refurbished MacBooks last just as long as new ones - these are built to run for years."},
    {q: "What's your refurbishment process?", a: "Professional 47-point inspection, deep cleaning, testing, and certification by Apple-trained technicians."},
    {q: "What keyboard options are available?", a: `We offer ${keyboardInfo} to suit Canadian users perfectly.`}
  ];
  
  const selectedFAQs = [...faqPool].sort(() => 0.5 - Math.random()).slice(0, 4);

  // Continue with the rest of the description...
 return `
  <div class="macbook-depot-product">
    <div class="hero-section">
      🔥 <strong>SAVE UP TO 39% OFF RETAIL!</strong><br>
      💎 ${productInfo.productType}${productInfo.displaySize ? ` ${productInfo.displaySize}` : ''} ${productInfo.processor} ${productInfo.year} - Starting from $${Math.round(lowestPrice).toLocaleString()}<br>
      <small>Retail Price: $${dynamicRetailPrice.toLocaleString()}</small>
    </div>

    <h2>⚡ Why Choose This ${productInfo.productType}?</h2>
    <p>${audienceAndUse} ${productInfo.storage ? `With ${productInfo.storage} storage` : ''}${productInfo.memory ? ` and ${productInfo.memory} memory` : ''}, this powerhouse delivers professional-grade performance at an unbeatable price.</p>

    <h2>💰 Smart Savings by Condition Grade</h2>
    ★ <strong>Grade ${primaryGrade} (Most Available): $${Math.round(primaryPrice).toLocaleString()} - YOUR BEST VALUE</strong><br>
    • <strong>Grade A (Like-New):</strong> $${Math.round(gradeAPrice).toLocaleString()} - Save $${Math.round(dynamicRetailPrice - gradeAPrice).toLocaleString()} (30%) - Perfect condition<br>
    • <strong>Grade B (Excellent):</strong> $${Math.round(gradeBPrice).toLocaleString()} - Save $${Math.round(dynamicRetailPrice - gradeBPrice).toLocaleString()} (32%) - Minimal wear<br>
    • <strong>Grade C (Good):</strong> $${Math.round(gradeCPrice).toLocaleString()} - Save $${Math.round(dynamicRetailPrice - gradeCPrice).toLocaleString()} (34%) - Great value<br>
    • <strong>Grade D (Fair):</strong> $${Math.round(gradeDPrice).toLocaleString()} - Save $${Math.round(dynamicRetailPrice - gradeDPrice).toLocaleString()} (39%) - Maximum savings

    <h2>🎯 What You Get</h2>
    <ul>
      <li>✅ ${productInfo.productType}${productInfo.displaySize ? ` ${productInfo.displaySize}` : ''} with ${productInfo.processor} processor</li>
      ${productInfo.storage ? `<li>✅ ${productInfo.storage} high-speed SSD storage</li>` : ''}
      ${productInfo.memory ? `<li>✅ ${productInfo.memory} unified memory for seamless multitasking</li>` : ''}
      ${productInfo.displaySize ? `<li>✅ Stunning ${productInfo.displaySize} Retina display with True Tone</li>` : ''}
      <li>✅ ${keyboardInfo} included</li>
      <li>✅ Original Apple charger and documentation</li>
      <li>✅ Professional cleaning and testing certification</li>
    </ul>

    <h2>🛡️ Risk-Free MacBookDepot Guarantee</h2>
    <ul>
  <li>🛡️ <strong>Industry-leading 90-day warranty</strong> - Most competitors offer only 30 days. We stand behind our quality.</li>
  <li>⚡ <strong>Fast North American shipping</strong> - Order by 2 PM, ships same day. No waiting weeks like other sellers</li>
  <li>🔄 <strong>Risk-free 30-day returns</strong> - Don't love it? Return it for a full refund, no questions asked</li>
  <li>🔧 <strong>47-point inspection process</strong> - Every device tested by Apple-certified technicians before shipping</li>
  <li>📞 <strong>Real human CHAT support</strong> - Talk to actual tech experts, not chatbots or overseas call centers</li>
  <li>🌱 <strong>Environmental impact</strong> - Each refurbished device prevents 300kg of CO2 emissions vs buying new</li>
  <li>🏆 <strong>Trusted by 50,000+ customers</strong> - Join thousands who've saved money without sacrificing quality</li>
  <li>💎 <strong>Grade transparency</strong> - Honest condition ratings so you know exactly what you're getting</li>
</ul>
	
	<h2>🔧 Complete Technical Specifications</h2>
    <div class="specs-container">
      <div class="specs-grid">
        
        <div class="spec-category">
          <h3>⚡ Performance</h3>
          <div class="spec-table">
            <div class="spec-row">
              <span class="spec-label">Processor</span>
              <span class="spec-value">${productInfo.processor} chip with Neural Engine</span>
            </div>
            ${productInfo.memory ? `
            <div class="spec-row">
              <span class="spec-label">Memory (RAM)</span>
              <span class="spec-value">${productInfo.memory} unified memory</span>
            </div>` : ''}
            <div class="spec-row">
              <span class="spec-label">Graphics</span>
              <span class="spec-value">${productInfo.processor?.includes('Max') ? 'High-performance GPU (up to 32-core)' : productInfo.processor?.includes('Pro') ? 'Professional GPU (up to 19-core)' : 'Integrated GPU (up to 10-core)'}</span>
            </div>
          </div>
        </div>

        <div class="spec-category">
          <h3>💾 Storage & Display</h3>
          <div class="spec-table">
            ${productInfo.storage ? `
            <div class="spec-row">
              <span class="spec-label">Storage</span>
              <span class="spec-value">${productInfo.storage} SSD (ultra-fast)</span>
            </div>` : ''}
            ${productInfo.displaySize ? `
            <div class="spec-row">
              <span class="spec-label">Display Size</span>
              <span class="spec-value">${productInfo.displaySize} Liquid Retina display</span>
            </div>
            <div class="spec-row">
              <span class="spec-label">Resolution</span>
              <span class="spec-value">${productInfo.displaySize === '16"' ? 'Ultra-high resolution (3456×2234)' : 'High resolution (2560×1600)'}</span>
            </div>` : ''}
            <div class="spec-row">
              <span class="spec-label">Display Features</span>
              <span class="spec-value">True Tone, P3 wide color, anti-reflective coating</span>
            </div>
          </div>
        </div>

        <div class="spec-category">
          <h3>🔌 Connectivity</h3>
          <div class="spec-table">
            <div class="spec-row">
              <span class="spec-label">Thunderbolt Ports</span>
              <span class="spec-value">${productInfo.displaySize === '16"' ? 'Multiple Thunderbolt 4' : '2x Thunderbolt 4'} (USB-C)</span>
            </div>
            <div class="spec-row">
              <span class="spec-label">Wireless</span>
              <span class="spec-value">Wi-Fi 6, Bluetooth 5.0</span>
            </div>
            <div class="spec-row">
              <span class="spec-label">Keyboard</span>
              <span class="spec-value">${keyboardInfo} with Touch ID</span>
            </div>
          </div>
        </div>

        <div class="spec-category">
          <h3>🔋 Power & Design</h3>
          <div class="spec-table">
            <div class="spec-row">
              <span class="spec-label">Battery Life</span>
              <span class="spec-value">${productInfo.displaySize === '16"' ? 'All-day (up to 21 hours)' : 'All-day (up to 18 hours)'}</span>
            </div>
            <div class="spec-row">
              <span class="spec-label">Weight</span>
              <span class="spec-value">${productInfo.displaySize === '16"' ? 'Portable (2.1 kg / 4.7 lbs)' : 'Ultra-portable (1.4 kg / 3.0 lbs)'}</span>
            </div>
            <div class="spec-row">
              <span class="spec-label">Build Quality</span>
              <span class="spec-value">Premium aluminum unibody design</span>
            </div>
          </div>
        </div>
      </div>
    </div>

    <h2>❓ Frequently Asked Questions</h2>
    ${selectedFAQs.map(faq => `
      <h3>${faq.q}</h3>
      <p>${faq.a}</p>
    `).join('')}
	
	<h2>🔗 Explore Similar Models</h2>
    <div class="internal-links">
      ${productInfo.productType.includes('MacBook Pro') ? `
        • Need more portability? <a href="/collections/macbook-air">MacBook Air Collection</a><br>
        • Want different specs? <a href="/collections/macbook-pro">All MacBook Pro Models</a><br>
        • Budget-conscious? <a href="/collections/grade-c-macbooks">Grade C MacBooks</a><br>
        • Latest processors? <a href="/collections/apple-silicon">Apple Silicon Devices</a>
      ` : productInfo.productType.includes('MacBook Air') ? `
        • Need more power? <a href="/collections/macbook-pro">MacBook Pro Collection</a><br>
        • Larger screen? <a href="/collections/large-screen">Large Screen MacBooks</a><br>
        • Budget options? <a href="/collections/grade-d-macbooks">Grade D MacBooks</a><br>
        • Professional grade? <a href="/collections/macbook-pro">MacBook Pro Models</a>
      ` : `
        • Explore all models: <a href="/collections/macbook">MacBook Collection</a><br>
        • Budget-friendly: <a href="/collections/grade-c-macbooks">Grade C Devices</a><br>
        • Latest chips: <a href="/collections/apple-silicon">Apple Silicon Collection</a><br>
        • Professional grade: <a href="/collections/macbook-pro">MacBook Pro Models</a>
      `}
    </div>

    <div class="urgency-footer">
      ⏰ <strong>Limited stock available</strong> - Refurbished Apple devices sell fast. <strong>Secure yours today!</strong>
    </div>
  </div>

<style>
    .hero-section {
      background: linear-gradient(45deg, #F26A38, #0085B1);
      color: white;
      padding: 20px;
      border-radius: 8px;
      text-align: center;
      margin-bottom: 20px;
      font-size: 1.1em;
    }
    .urgency-footer {
      background: #fff3cd;
      border: 1px solid #ffc107;
      padding: 15px;
      border-radius: 5px;
      text-align: center;
      margin-top: 20px;
      font-weight: bold;
    }
    .macbook-depot-product h2 {
      color: #333;
      margin-top: 25px;
      margin-bottom: 15px;
    }
    .macbook-depot-product h3 {
      color: #555;
      margin-top: 15px;
      margin-bottom: 8px;
      font-size: 1.1em;
    }
    .macbook-depot-product ul {
      margin-left: 20px;
    }
    .macbook-depot-product a {
      color: #007bff;
      text-decoration: none;
    }
    .macbook-depot-product a:hover {
      text-decoration: underline;
    }
    .specs-container {
      background: #f8f9fa;
      border-radius: 12px;
      padding: 25px;
      margin: 25px 0;
    }
    .specs-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 20px;
      margin-bottom: 20px;
    }
    .spec-category {
      background: white;
      border-radius: 8px;
      padding: 15px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    }
    .spec-category h3 {
      margin: 0 0 12px 0;
      color: #333;
      font-size: 1.1em;
      border-bottom: 2px solid #007bff;
      padding-bottom: 5px;
    }
    .spec-table {
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .spec-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 8px 0;
      border-bottom: 1px solid #eee;
    }
    .spec-row:last-child {
      border-bottom: none;
    }
    .spec-label {
      font-weight: 600;
      color: #555;
      flex: 1;
    }
    .spec-value {
      color: #333;
      flex: 1.5;
      text-align: right;
      font-weight: 500;
    }
    .internal-links {
      background: #e8f4fd;
      padding: 15px;
      border-radius: 8px;
      border-left: 4px solid #007bff;
    }
    @media (max-width: 768px) {
      .specs-grid {
        grid-template-columns: 1fr;
      }
      .spec-row {
        flex-direction: column;
        align-items: flex-start;
        gap: 4px;
      }
      .spec-value {
        text-align: left;
      }
    }
  </style>`;
}

function calculateAdvancedPricing(productInfo) {
  const basePrices = {
    'MacBook Pro': 2299,
    'MacBook Air': 1399,
    'MacBook': 1599,
    'iPad Pro': 1149,
    'iPad Air': 779,
    'iPad': 449,
    'iPad Mini': 649,
    'iPhone': 899,
    'iMac': 1699,
    'Mac Studio': 2499,
    'Mac Mini': 799,
    'AirPods': 199,
    'Magic Mouse': 99,
    'Magic Keyboard': 199,
    'Apple Accessory': 99
  };
  
  let basePrice = basePrices[productInfo.productType] || 999;
  
  // Adjust for specifications
  if (productInfo.storage) {
    const storageNum = parseInt(productInfo.storage);
    if (storageNum >= 2000) basePrice += 800; // 2TB+
    else if (storageNum >= 1000) basePrice += 400; // 1TB+
    else if (storageNum >= 512) basePrice += 200; // 512GB
  }
  
  if (productInfo.memory) {
    const memoryNum = parseInt(productInfo.memory);
    if (memoryNum >= 64) basePrice += 1000; // 64GB+
    else if (memoryNum >= 32) basePrice += 600; // 32GB+
    else if (memoryNum >= 16) basePrice += 400; // 16GB
  }
  
  // Processor adjustments
  if (productInfo.processor.includes('Max')) basePrice += 800;
  else if (productInfo.processor.includes('Pro')) basePrice += 400;
  else if (productInfo.processor.includes('Ultra')) basePrice += 1200;
  
  return Math.round(basePrice);
}

function createAdvancedCollections(productInfo) {
  const collections = [];
  
  // Main product type
  collections.push(productInfo.productType);
  
  // Category
  collections.push(productInfo.category);
  
  // Device family
  if (productInfo.deviceFamily) {
    collections.push(productInfo.deviceFamily);
  }
  
  // Processor-based collections
  if (productInfo.processor) {
    if (productInfo.processor.includes('M3')) collections.push('M3 Chip Devices');
    else if (productInfo.processor.includes('M2')) collections.push('M2 Chip Devices');
    else if (productInfo.processor.includes('M1')) collections.push('M1 Chip Devices');
    else if (productInfo.processor.includes('Intel')) collections.push('Intel Mac');
  }
  
  // Year-based
  if (productInfo.year) {
    collections.push(`${productInfo.year} Models`);
  }
  
  // Size-based for devices with displays
  if (productInfo.displaySize) {
    const size = parseFloat(productInfo.displaySize);
    if (size >= 15) collections.push('Large Screen');
    else if (size >= 13) collections.push('Standard Screen');
    else if (size > 0) collections.push('Compact');
  }
  
  // Universal collections
  collections.push('Certified Refurbished');
  collections.push('Apple');
  
  return [...new Set(collections)];
}

function createAdvancedTags(productInfo) {
  const tags = ['refurbished', 'apple', 'certified'];
  
  // Product type tags
  tags.push(productInfo.productType.toLowerCase().replace(/\s+/g, '-'));
  
  // Processor tags with enhanced chip detection
  if (productInfo.processor) {
    tags.push(productInfo.processor.toLowerCase().replace(/\s+/g, '-'));
    
    // Enhanced chip family tags
    if (productInfo.processor.includes('M4')) tags.push('m4-chip');
    else if (productInfo.processor.includes('M3')) tags.push('m3-chip');
    else if (productInfo.processor.includes('M2')) tags.push('m2-chip');
    else if (productInfo.processor.includes('M1')) tags.push('m1-chip');
    
    // Chip tier tags
    if (productInfo.processor.includes('Ultra')) tags.push('ultra-chip');
    else if (productInfo.processor.includes('Max')) tags.push('max-chip');
    else if (productInfo.processor.includes('Pro')) tags.push('pro-chip');
    
    // Apple Silicon vs Intel
    if (productInfo.processor.match(/M[1-4]/)) {
      tags.push('apple-silicon');
    } else if (productInfo.processor.includes('Intel')) {
      tags.push('intel');
    }
  }
  
  // Specifications tags
  if (productInfo.storage) tags.push(productInfo.storage.toLowerCase());
  if (productInfo.memory) tags.push(productInfo.memory.toLowerCase().replace('gb', 'gb-ram'));
  if (productInfo.displaySize) tags.push(productInfo.displaySize.toLowerCase().replace(/[^\w]/g, ''));
  if (productInfo.year) tags.push(productInfo.year);
  
  // Keyboard language tags (NEW)
  if (productInfo.keyboardLayout) {
    if (productInfo.keyboardLayout === 'French Canadian') {
      tags.push('french-canadian', 'french-keyboard', 'bilingual', 'qwerty-french', 'fr-ca');
    } else {
      tags.push('english-canadian', 'english-keyboard', 'canadian-english', 'en-ca');
    }
  }
  
  // Device family tags
  if (productInfo.deviceFamily) {
    tags.push(productInfo.deviceFamily.toLowerCase().replace(/\s+/g, '-'));
  }
  
  // Category tags
  tags.push(productInfo.category.toLowerCase());
  
  // Regional tags for Canadian market
  tags.push('canada', 'canadian', 'macbook-depot');
  
  return tags;
}

// VARIANT MANAGEMENT FUNCTIONS
function determineKeyboardLayout(item) {
  // Check comments, model, or other fields for French/English indicators
  const allText = `${item['Model'] || ''} ${item['Comments'] || ''} ${item['Processor'] || ''}`.toLowerCase();
  
  if (allText.includes('french') || allText.includes('français') || 
      allText.includes('fr-ca') || allText.includes('canadian french')) {
    return 'French Canadian';
  }
  
  // Default to English
  return 'English';
}

function cleanColor(color) {
  if (!color) return 'Space Gray';
  
  const colorMap = {
    'space grey': 'Space Gray',
    'space gray': 'Space Gray', 
    'spacegrey': 'Space Gray',
    'spacegray': 'Space Gray',
    'silver': 'Silver',
    'gold': 'Gold',
    'rose gold': 'Rose Gold',
    'midnight': 'Midnight',
    'starlight': 'Starlight',
    'default': 'Space Gray'
  };
  
  const cleanedColor = color.toString().toLowerCase().trim();
  return colorMap[cleanedColor] || color.toString().trim();
}

function cleanCondition(condition) {
  if (!condition) return 'A';
  
  // Extract just the letter grade, handle various formats
  const conditionStr = condition.toString().toUpperCase().trim();
  const match = conditionStr.match(/[ABCD]/);
  return match ? match[0] : 'A';
}

function createAdvancedSKU(productInfo, color, condition, keyboardLayout) {
  const components = [
    productInfo.productType.replace(/\s+/g, '').substring(0, 4).toUpperCase(),
    productInfo.displaySize ? productInfo.displaySize.replace(/[^\d]/g, '').substring(0, 2) : '',
    productInfo.processor ? productInfo.processor.replace(/\s+/g, '').substring(0, 3).toUpperCase() : '',
    productInfo.storage ? productInfo.storage.replace(/[^\d]/g, '') : '',
    color.substring(0, 2).toUpperCase(),
    condition.toUpperCase(),
    keyboardLayout === 'French Canadian' ? 'FR' : 'EN'
  ].filter(Boolean);
  
  return components.join('-');
}

function calculateVariantPrice(productInfo, condition) {
  const retailPrice = calculateRetailPrice(productInfo);
  const conditionMultipliers = {
    'A': 0.70,    // 30% off retail
    'B': 0.68,    // 32% off retail  
    'C': 0.66,    // 34% off retail
    'D': 0.61     // 39% off retail (30% + 5% + 4%)
  };
  
  return Math.round(retailPrice * (conditionMultipliers[condition] || 0.70));
}

function calculateComparePrice(productInfo, condition) {
  // Show retail price as "compare at" price for all conditions
  return calculateRetailPrice(productInfo);
}

function createSEOOptimizedHandle(productInfo) {
  // Create URL handle: refurbished-macbook-pro-13-m2-2022-512gb-16gb
  const components = [
    'refurbished', // Always start with refurbished
    productInfo.productType?.toLowerCase().replace(/\s+/g, '-'),
    productInfo.displaySize?.replace(/[^0-9.]/g, ''),
    productInfo.processor?.toLowerCase()
      .replace(/apple\s+/g, '')
      .replace(/\s+chip/g, '')
      .replace(/\s+/g, '-'),
    productInfo.year,
    productInfo.storage?.toLowerCase(),
    productInfo.memory?.toLowerCase()
  ].filter(Boolean);
  
  return components
    .join('-')
    .replace(/[^a-z0-9-]/g, '') // Remove special chars
    .replace(/-+/g, '-')        // Replace multiple hyphens
    .replace(/^-|-$/g, '')      // Remove leading/trailing hyphens
    .substring(0, 255);         // Shopify limit
}

function getAdvancedConditionDescription(condition) {
  const descriptions = {
    'A': 'Excellent condition - Like new appearance with minimal wear. Perfect for professionals who want the best.',
    'B': 'Very good condition - Light cosmetic wear but excellent functionality. Great balance of value and quality.',
    'C': 'Good condition - Visible wear but fully functional. Ideal for budget-conscious buyers who want reliability.',
    'D': 'Fair condition - Heavy wear but guaranteed functionality. Maximum savings for those who prioritize price.'
  };

  return descriptions[condition] || descriptions['A'];
}

// =============================================================================
// ADVANCED SHOPIFY SYNC FUNCTIONS
// =============================================================================

function findExistingProductAdvanced(existingProducts, productGroup) {
  const searchTitle = productGroup.seoTitle.toLowerCase();

  return existingProducts.find(product => {
    const productTitle = product.title.toLowerCase();

    // ONLY do exact title matching - this is the safest and simplest approach
    if (productTitle === searchTitle) {
      console.log(`🎯 Exact title match found: ${product.title}`);
      return true;
    }

    // No partial matching - if titles don't match exactly, treat as new product
    return false;
  });
}



async function createNewProductAdvanced(baseUrl, headers, productGroup, collections) {
  console.log(`🆕 Creating product with ${Object.keys(productGroup.variants).length} variants...`);

  // Create variants with stock tracking
  const variants = await createAdvancedVariants(productGroup);
  
  // Create option values from variants
  const colorOptions = [...new Set(variants.map(v => v.option1))];
  const conditionOptions = [...new Set(variants.map(v => v.option2))];
  const keyboardOptions = [...new Set(variants.map(v => v.option3))];

  // Create SEO-optimized handle
  const seoHandle = createSEOOptimizedHandle({
    productType: productGroup.productType,
    displaySize: productGroup.displaySize,
    processor: productGroup.processor,
    year: productGroup.year,
    storage: productGroup.storage,
    memory: productGroup.memory
  });

  console.log(`🎯 SEO Handle: ${seoHandle}`);

  const productData = {
    product: {
      title: productGroup.seoTitle,
      body_html: productGroup.productDescription,
      vendor: 'Apple',
      product_type: productGroup.productType,
      status: 'active',
      handle: seoHandle,
      options: [
        { name: 'Color', values: colorOptions },
        { name: 'Condition', values: conditionOptions },
        { name: 'Keyboard', values: keyboardOptions }
      ],
      variants: variants,
      tags: productGroup.tags.join(', '),
      seo_title: productGroup.seoTitle,
      seo_description: productGroup.seoDescription
    }
  };

  console.log(`📦 Sending product data: ${variants.length} variants`);

  const response = await fetch(`${baseUrl}products.json`, {
    method: 'POST',
    headers,
    body: JSON.stringify(productData)
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to create product: ${response.status} - ${errorText}`);
  }

  const createdProduct = await response.json();
  console.log(`✅ Product created with ID: ${createdProduct.product.id}`);

  // NEW: Upload images if available
  let imagesUploaded = 0;
  if (productGroup.imageAvailability && productGroup.imageAvailability.hasImages) {
    console.log(`📸 Uploading images for product ${createdProduct.product.id}...`);
    imagesUploaded = await uploadProductImages(baseUrl, headers, createdProduct.product.id, productGroup.imageAvailability.bestMatch);
  }

  // Add to collections
  await addProductToCollectionsAdvanced(
    baseUrl, headers, createdProduct.product.id, productGroup.collections, collections
  );

  return {
    product: createdProduct.product,
    variantsCreated: variants.length,
    stockItemsProcessed: productGroup.totalUnits,
    imagesUploaded: imagesUploaded
  };
}


// NEW: Upload images to Shopify product
async function uploadProductImages(baseUrl, headers, productId, bestMatch) {
  if (!bestMatch || !bestMatch.images || bestMatch.images.length === 0) {
    console.log('⚪ No images to upload');
    return 0;
  }

  console.log(`📷 Uploading ${bestMatch.images.length} images to product ${productId}...`);
  let uploadedCount = 0;

  for (let i = 0; i < Math.min(bestMatch.images.length, 8); i++) {
    const image = bestMatch.images[i];
    
    try {
      console.log(`📤 Uploading image ${i + 1}: ${image.filename}`);
      
      const imageData = {
        image: {
          product_id: productId,
          src: image.url,
          alt: image.seoAltTag || image.imageDescription || `Product image ${i + 1}`,
          filename: image.filename
        }
      };

      const imageResponse = await fetch(`${baseUrl}products/${productId}/images.json`, {
        method: 'POST',
        headers,
        body: JSON.stringify(imageData)
      });

      if (imageResponse.ok) {
        const uploadedImage = await imageResponse.json();
        console.log(`✅ Image ${i + 1} uploaded: ${image.filename}`);
        uploadedCount++;
      } else {
        const errorText = await imageResponse.text();
        console.log(`❌ Image ${i + 1} upload failed: ${imageResponse.status} - ${errorText}`);
      }

      await new Promise(resolve => setTimeout(resolve, 600));

    } catch (error) {
      console.log(`❌ Error uploading image ${i + 1}: ${error.message}`);
    }
  }

  console.log(`📸 Images uploaded: ${uploadedCount}/${Math.min(bestMatch.images.length, 8)}`);
  return uploadedCount;
}








async function updateExistingProductAdvanced(baseUrl, headers, existingProduct, productGroup, collections) {
  const startTime = Date.now();
  console.log(`\n🔄 === STARTING DETAILED UPDATE PROCESS ===`);
  console.log(`📋 Product: ${productGroup.seoTitle}`);
  console.log(`🆔 Existing Product ID: ${existingProduct.id}`);
  console.log(`📊 Current variants in Shopify: ${existingProduct.variants?.length || 0}`);
  console.log(`📦 New stock items to process: ${productGroup.totalUnits}`);
  console.log(`🎨 New variants to create/update: ${Object.keys(productGroup.variants).length}`);

  try {
    // Step 1: Update product details
    console.log(`\n📝 STEP 1: Updating product details...`);
    const updateData = {
      product: {
        id: existingProduct.id,
        title: productGroup.seoTitle,
        body_html: productGroup.productDescription,
        tags: productGroup.tags.join(', '),
        seo_title: productGroup.seoTitle,
        seo_description: productGroup.seoDescription
      }
    };

    console.log(`🔗 API Call: PUT ${baseUrl}products/${existingProduct.id}.json`);
    console.log(`📤 Request data: ${JSON.stringify(updateData, null, 2)}`);
    
    const apiStartTime = Date.now();
    const productUpdateResponse = await fetch(`${baseUrl}products/${existingProduct.id}.json`, {
      method: 'PUT',
      headers,
      body: JSON.stringify(updateData)
    });
    const apiEndTime = Date.now();

    console.log(`⏱️ API Response time: ${apiEndTime - apiStartTime}ms`);
    console.log(`📥 Response status: ${productUpdateResponse.status} ${productUpdateResponse.statusText}`);

    if (!productUpdateResponse.ok) {
      const errorText = await productUpdateResponse.text();
      console.log(`❌ Product update failed - Response: ${errorText}`);
      throw new Error(`Product update failed: ${productUpdateResponse.status} - ${errorText}`);
    }

    const updatedProductData = await productUpdateResponse.json();
    console.log(`✅ Product details updated successfully`);
    console.log(`📋 Updated product title: "${updatedProductData.product.title}"`);

    // Step 2: Process variants
    console.log(`\n🎨 STEP 2: Processing variants...`);
    const newVariants = await createAdvancedVariants(productGroup);
    console.log(`📊 Generated ${newVariants.length} variants from ${productGroup.totalUnits} stock items`);
    
    let variantsUpdated = 0;
    let variantsCreated = 0;
    let totalInventoryAdded = 0;

    console.log(`\n🔄 Processing each variant:`);

    for (let i = 0; i < newVariants.length; i++) {
      const newVariant = newVariants[i];
      console.log(`\n--- Variant ${i + 1}/${newVariants.length}: ${newVariant.title} ---`);
      console.log(`🎯 Looking for existing variant with:`);
      console.log(`   Option1 (Color): "${newVariant.option1}"`);
      console.log(`   Option2 (Condition): "${newVariant.option2}"`);
      console.log(`   Option3 (Keyboard): "${newVariant.option3}"`);
      console.log(`📦 New inventory quantity: ${newVariant.inventory_quantity}`);
      console.log(`💰 Price: $${newVariant.price}`);
      console.log(`🏷️ SKU: ${newVariant.sku}`);
      
      try {
        // Find matching existing variant
        const existingVariant = existingProduct.variants.find(v => 
          v.option1 === newVariant.option1 && 
          v.option2 === newVariant.option2 && 
          v.option3 === newVariant.option3
        );

        if (existingVariant) {
          console.log(`✅ Found existing variant ID: ${existingVariant.id}`);
          console.log(`📊 Current inventory: ${existingVariant.inventory_quantity || 0}`);
          
          // Calculate new inventory (ADD to existing)
          const currentInventory = existingVariant.inventory_quantity || 0;
          const addingInventory = parseInt(newVariant.inventory_quantity);
          const newInventory = currentInventory + addingInventory;
          
          console.log(`🔢 Inventory calculation: ${currentInventory} + ${addingInventory} = ${newInventory}`);
          
          const variantUpdateData = {
            variant: {
              id: existingVariant.id,
              inventory_quantity: newInventory,
              price: newVariant.price,
              compare_at_price: newVariant.compare_at_price,
              sku: newVariant.sku
            }
          };

          console.log(`🔗 API Call: PUT ${baseUrl}variants/${existingVariant.id}.json`);
          console.log(`📤 Update data: ${JSON.stringify(variantUpdateData, null, 2)}`);

          const variantApiStart = Date.now();
          const variantUpdateResponse = await fetch(`${baseUrl}variants/${existingVariant.id}.json`, {
            method: 'PUT',
            headers,
            body: JSON.stringify(variantUpdateData)
          });
          const variantApiEnd = Date.now();

          console.log(`⏱️ Variant API time: ${variantApiEnd - variantApiStart}ms`);
          console.log(`📥 Variant response: ${variantUpdateResponse.status} ${variantUpdateResponse.statusText}`);

          if (variantUpdateResponse.ok) {
            const updatedVariantData = await variantUpdateResponse.json();
            console.log(`✅ Variant updated successfully`);
            console.log(`📊 Final inventory: ${updatedVariantData.variant.inventory_quantity}`);
            console.log(`💰 Final price: $${updatedVariantData.variant.price}`);
            variantsUpdated++;
            totalInventoryAdded += addingInventory;
          } else {
            const errorText = await variantUpdateResponse.text();
            console.log(`❌ Variant update failed: ${variantUpdateResponse.status}`);
            console.log(`📝 Error response: ${errorText}`);
            throw new Error(`Variant update failed: ${variantUpdateResponse.status} - ${errorText}`);
          }
        } else {
          console.log(`🆕 No existing variant found - creating new variant`);
          
          const variantCreateData = {
            variant: {
              ...newVariant,
              product_id: existingProduct.id
            }
          };

          console.log(`🔗 API Call: POST ${baseUrl}products/${existingProduct.id}/variants.json`);
          console.log(`📤 Create data: ${JSON.stringify(variantCreateData, null, 2)}`);

          const createApiStart = Date.now();
          const variantCreateResponse = await fetch(`${baseUrl}products/${existingProduct.id}/variants.json`, {
            method: 'POST',
            headers,
            body: JSON.stringify(variantCreateData)
          });
          const createApiEnd = Date.now();

          console.log(`⏱️ Create API time: ${createApiEnd - createApiStart}ms`);
          console.log(`📥 Create response: ${variantCreateResponse.status} ${variantCreateResponse.statusText}`);

          if (variantCreateResponse.ok) {
            const createdVariantData = await variantCreateResponse.json();
            console.log(`✅ New variant created with ID: ${createdVariantData.variant.id}`);
            console.log(`📊 Inventory set to: ${createdVariantData.variant.inventory_quantity}`);
            console.log(`💰 Price set to: $${createdVariantData.variant.price}`);
            variantsCreated++;
            totalInventoryAdded += parseInt(newVariant.inventory_quantity);
          } else {
            const errorText = await variantCreateResponse.text();
            console.log(`❌ Variant creation failed: ${variantCreateResponse.status}`);
            console.log(`📝 Error response: ${errorText}`);
            throw new Error(`Variant creation failed: ${variantCreateResponse.status} - ${errorText}`);
          }
        }

        // Rate limiting
        console.log(`⏳ Rate limiting pause (300ms)...`);
        await new Promise(resolve => setTimeout(resolve, 300));

      } catch (variantError) {
        console.log(`❌ Error processing variant ${newVariant.title}:`);
        console.log(`🔍 Variant error details: ${variantError.message}`);
        throw variantError;
      }
    }

    const endTime = Date.now();
    const totalTime = endTime - startTime;

    console.log(`\n🎉 === UPDATE PROCESS COMPLETE ===`);
    console.log(`⏱️ Total time: ${totalTime}ms`);
    console.log(`📊 Results summary:`);
    console.log(`   • Variants updated: ${variantsUpdated}`);
    console.log(`   • Variants created: ${variantsCreated}`);
    console.log(`   • Total inventory added: ${totalInventoryAdded} units`);
    console.log(`   • Stock items processed: ${productGroup.totalUnits}`);
    console.log(`✅ Update successful for: ${productGroup.seoTitle}`);

    return {
      product: existingProduct,  // ✅ USE existingProduct instead
      variantsUpdated: variantsUpdated,  // ✅ Use variantsUpdated instead of variantsCreated
      stockItemsProcessed: productGroup.totalUnits,
      imagesUploaded: 0  // ✅ Set to 0 for updates (or add image upload logic later)
    };

  } catch (error) {
    const endTime = Date.now();
    const totalTime = endTime - startTime;
    
    console.log(`\n❌ === UPDATE PROCESS FAILED ===`);
    console.log(`⏱️ Failed after: ${totalTime}ms`);
    console.log(`🔍 Final error: ${error.message}`);
    console.log(`📋 Product: ${productGroup.seoTitle}`);
    console.log(`🆔 Product ID: ${existingProduct.id}`);
    console.log(`📊 Attempted variants: ${Object.keys(productGroup.variants).length}`);
    console.log(`📦 Attempted stock items: ${productGroup.totalUnits}`);
    
    throw error;
  }
}

// Replace the createAdvancedVariants function (around line 1850-1950) with this:

async function createAdvancedVariants(productGroup) {
  const variants = [];
  const variantMap = {}; // To aggregate variants with same color/condition/keyboard

  console.log(`🎨 Creating aggregated variants for ${productGroup.seoTitle} with ${productGroup.totalUnits} total units`);
  console.log(`📦 Processing stock items and aggregating by variant type`);

  // Process each stock item and aggregate by variant type
  if (productGroup.stockItems && productGroup.stockItems.length > 0) {
    productGroup.stockItems.forEach((stockItem, index) => {
      const { stockId, serialNumber, condition, color, keyboardLayout } = stockItem;
      
      // Skip if missing critical data
      if (!stockId || !serialNumber) {
        console.log(`⚠️ Skipping stock item ${index + 1} - missing stock ID or serial`);
        return;
      }

      // Create variant key WITHOUT stock number for aggregation
      const variantKey = `${color}|${condition}|${keyboardLayout}`;
      
      // Initialize variant if it doesn't exist
      if (!variantMap[variantKey]) {
        variantMap[variantKey] = {
          color: color,
          condition: condition,
          keyboardLayout: keyboardLayout,
          inventory_quantity: 0,
          stockItems: [], // Keep track of individual items for reference
          skus: [], // Collect all stock IDs for SKU
          barcodes: [] // Collect all serial numbers
        };
      }
      
      // Add to aggregated variant
      variantMap[variantKey].inventory_quantity += 1;
      variantMap[variantKey].stockItems.push(stockItem);
      variantMap[variantKey].skus.push(stockId.toString());
      variantMap[variantKey].barcodes.push(serialNumber);
    });
  }

  // Convert aggregated variants to Shopify format
  Object.entries(variantMap).forEach(([key, variantData]) => {
    const { color, condition, keyboardLayout, inventory_quantity, skus, barcodes } = variantData;
    
    // Create clean variant title without stock numbers
    const variantTitle = `${color} - Grade ${condition} - ${keyboardLayout}`;
    
    // Calculate pricing based on condition
    const price = calculateVariantPrice(productGroup, condition);
    const compareAtPrice = calculateComparePrice(productGroup, condition);
    
    // Create aggregated SKU (use first stock number or create a combined one)
    // You could also join all SKUs with a delimiter if needed
    const sku = skus.join('-'); // Or just use skus[0] for the first one
    const barcode = barcodes[0]; // Use first serial number as barcode
    
    console.log(`✅ Creating aggregated variant: ${variantTitle} | Quantity: ${inventory_quantity} | SKUs: ${skus.join(', ')}`);
    
    variants.push({
      title: variantTitle,
      option1: color,
      option2: `Grade ${condition}`,
      option3: keyboardLayout, // Just "English" or "French Canadian", no stock number
      inventory_quantity: inventory_quantity, // Total quantity for this variant
      inventory_management: 'shopify',
      inventory_policy: 'deny',
      sku: sku, // Aggregated SKU
      barcode: barcode, // First barcode or you could leave empty
      price: price.toString(),
      compare_at_price: compareAtPrice > price ? compareAtPrice.toString() : null,
      weight: estimateWeight(productGroup.productType),
      weight_unit: 'kg',
      requires_shipping: true,
      taxable: true,
      fulfillment_service: 'manual'
    });
  });

  // Sort variants for consistency (optional)
  variants.sort((a, b) => {
    // Sort by color, then condition, then keyboard
    if (a.option1 !== b.option1) return a.option1.localeCompare(b.option1);
    if (a.option2 !== b.option2) return a.option2.localeCompare(b.option2);
    return a.option3.localeCompare(b.option3);
  });

  // If no variants were created, create a default one
  if (variants.length === 0) {
    console.log(`⚠️ No variants created, falling back to default variant`);
    variants.push({
      title: 'Default - Grade A - English',
      option1: 'Space Gray',
      option2: 'Grade A',
      option3: 'English',
      inventory_quantity: productGroup.totalUnits || 0,
      inventory_management: 'shopify',
      inventory_policy: 'deny',
      sku: createVariantSKU(productGroup, 'Space Gray', 'A', 'English', ''),
      price: productGroup.basePrice.toString(),
      weight: estimateWeight(productGroup.productType),
      weight_unit: 'kg',
      requires_shipping: true,
      taxable: true
    });
  }

  console.log(`📦 Final result: Created ${variants.length} unique variants with total inventory: ${variants.reduce((sum, v) => sum + parseInt(v.inventory_quantity), 0)} units`);
  
  // Log variant summary
  variants.forEach(v => {
    console.log(`   • ${v.title}: ${v.inventory_quantity} units`);
  });
  
  return variants;
}

function createVariantSKU(productGroup, color, condition, keyboardLayout, stockId) {
  const components = [
    productGroup.productType ? productGroup.productType.replace(/\s+/g, '').substring(0, 4).toUpperCase() : 'PROD',
    productGroup.displaySize ? productGroup.displaySize.replace(/[^\d]/g, '').substring(0, 2) : '',
    productGroup.processor ? productGroup.processor.replace(/\s+/g, '').substring(0, 3).toUpperCase() : '',
    productGroup.storage ? productGroup.storage.replace(/[^\d]/g, '') : '',
    color ? color.substring(0, 2).toUpperCase() : 'SG',
    condition ? condition.toUpperCase() : 'A',
    keyboardLayout === 'French Canadian' ? 'FR' : 'EN',
    stockId ? stockId.toString().slice(-4) : ''
  ].filter(Boolean);

  return components.join('-');
}

async function getAllExistingProductsAdvanced(baseUrl, headers) {
  let allProducts = [];
  let nextPageInfo = null;

  do {
    let url = `${baseUrl}products.json?limit=250&fields=id,title,handle,tags,variants,product_type`;
    if (nextPageInfo) {
      url += `&page_info=${nextPageInfo}`;
    }

    const response = await fetch(url, { headers });
    if (!response.ok) {
      throw new Error(`Failed to fetch products: ${response.status}`);
    }

    const data = await response.json();

    if (data.products) {
      allProducts = allProducts.concat(data.products);
    }

    // Check for next page
    const linkHeader = response.headers.get('Link');
    nextPageInfo = null;
    if (linkHeader && linkHeader.includes('rel="next"')) {
      const nextMatch = linkHeader.match(/<[^>]*[?&]page_info=([^&>]+)[^>]*>;\s*rel="next"/);
      if (nextMatch) {
        nextPageInfo = nextMatch[1];
      }
    }

    console.log(`📊 Fetched ${allProducts.length} products so far...`);

  } while (nextPageInfo);

  console.log(`✅ Total existing products fetched: ${allProducts.length}`);
  return allProducts;
}

async function setupCollectionsAdvanced(baseUrl, headers, productGroups) {
  console.log('🏗️ Setting up advanced collections...');

  // Get all unique collections needed
  const neededCollections = new Set();
  Object.values(productGroups).forEach(group => {
    if (group.collections) {
      group.collections.forEach(collection => neededCollections.add(collection));
    }
  });

  console.log(`📂 Need to ensure ${neededCollections.size} collections exist`);

  // Get existing collections
  const existingResponse = await fetch(`${baseUrl}custom_collections.json?limit=250`, { headers });
  if (!existingResponse.ok) {
    throw new Error(`Failed to fetch collections: ${existingResponse.status}`);
  }

  const existingData = await existingResponse.json();
  const existingCollections = existingData.custom_collections || [];

  const existingCollectionNames = existingCollections.map(c => c.title.toLowerCase());
  const collectionsMap = {};

  // Map existing collections
  existingCollections.forEach(collection => {
    collectionsMap[collection.title.toLowerCase()] = collection.id;
  });

  // Create missing collections
  for (const collectionName of neededCollections) {
    if (!existingCollectionNames.includes(collectionName.toLowerCase())) {
      try {
        const collectionData = {
          custom_collection: {
            title: collectionName,
            handle: collectionName.toLowerCase().replace(/[^a-z0-9]/g, '-').replace(/-+/g, '-'),
            published: true,
            sort_order: 'best-selling',
            body_html: `<p>Certified refurbished ${collectionName} devices with professional quality guarantee.</p>`
          }
        };

        const response = await fetch(`${baseUrl}custom_collections.json`, {
          method: 'POST',
          headers,
          body: JSON.stringify(collectionData)
        });

        if (response.ok) {
          const newCollection = await response.json();
          collectionsMap[collectionName.toLowerCase()] = newCollection.custom_collection.id;
          console.log(`✅ Created collection: ${collectionName}`);
        } else {
          console.log(`⚠️ Failed to create collection: ${collectionName}`);
        }

        // Rate limiting
        await new Promise(resolve => setTimeout(resolve, 300));
      } catch (error) {
        console.error(`❌ Error creating collection ${collectionName}:`, error.message);
      }
    } else {
      console.log(`✅ Collection already exists: ${collectionName}`);
    }
  }

  return collectionsMap;
}

async function addProductToCollectionsAdvanced(baseUrl, headers, productId, collectionNames, collectionsMap) {
  if (!collectionNames || collectionNames.length === 0) return;

  console.log(`🏷️ Adding product ${productId} to ${collectionNames.length} collections`);

  for (const collectionName of collectionNames) {
    const collectionId = collectionsMap[collectionName.toLowerCase()];
    if (collectionId) {
      try {
        const collectData = {
          collect: {
            product_id: productId,
            collection_id: collectionId
          }
        };

        const response = await fetch(`${baseUrl}collects.json`, {
          method: 'POST',
          headers,
          body: JSON.stringify(collectData)
        });

        if (response.ok) {
          console.log(`✅ Added to collection: ${collectionName}`);
        } else if (response.status === 422) {
          // Product already in collection, that's fine
          console.log(`ℹ️ Product already in collection: ${collectionName}`);
        } else {
          console.log(`⚠️ Failed to add to collection ${collectionName}: ${response.status}`);
        }

        // Rate limiting
        await new Promise(resolve => setTimeout(resolve, 200));
      } catch (error) {
        console.error(`❌ Error adding to collection ${collectionName}:`, error.message);
      }
    } else {
      console.log(`⚠️ Collection not found: ${collectionName}`);
    }
  }
}

function estimateWeight(productType) {
  const weights = {
    'MacBook Pro': 2.0,
    'MacBook Air': 1.3,
    'MacBook': 1.5,
    'iPad Pro': 0.7,
    'iPad Air': 0.6,
    'iPad': 0.5,
    'iPad Mini': 0.3,
    'iPhone': 0.2,
    'iMac': 4.5,
    'Mac Studio': 2.7,
    'Mac Mini': 1.2,
    'AirPods': 0.1,
    'Magic Mouse': 0.1,
    'Magic Keyboard': 0.3,
    'Apple Accessory': 0.2
  };

  return weights[productType] || 1.0;
}

function createSEOHandle(title) {
  // Use the new SEO-optimized handle creation - but this function is now deprecated
  // We'll use createSEOOptimizedHandle() instead, but keep this for compatibility
  return title
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, '') 
    .replace(/\s+/g, '-')         
    .replace(/-+/g, '-')          
    .replace(/^-|-$/g, '')        
    .substring(0, 255);           
}

// Start server
app.listen(PORT, () => {
  console.log(`🚀 MacBookDepot Enhanced Inventory Sync running at http://localhost:${PORT}`);
  console.log(`📁 Upload your Excel file and sync to Shopify with advanced features!`);
  console.log(`✨ Enhanced Features: Stock tracking, Variant management, Smart deduplication`);
  });


