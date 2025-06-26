// backend/server.js
// Load environment variables from .env file
require('dotenv').config();

// Import necessary modules
const express = require('express');
const admin = require('firebase-admin'); // Firebase Admin SDK
const cors = require('cors'); // CORS middleware
const multer = require('multer'); // Middleware for handling file uploads
const path = require('path'); // Node.js path module for file paths
const fs = require('fs'); // Node.js file system module
const { GoogleGenerativeAI } = require('@google/generative-ai'); // For Gemini API
const PDFDocument = require('pdfkit'); // For PDF generation
const ExcelJS = require('exceljs'); // For Excel generation
const { error } = require('console'); // For logging errors cleanly. (Added this for safety)

// --- Firebase Admin SDK Initialization ---

// Path to your service account key file relative to this server.js file
// Make sure firebase-service-account.json is in the same 'backend' directory.
const serviceAccount = require('./firebase-service-account.json');

// Initialize Firebase Admin SDK
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

// Get Firestore database instance
const db = admin.firestore();
console.log('[Backend Init] Firebase Admin SDK initialized and connected to Firestore.');

// --- Express Application Setup ---

const app = express();
// Backend will listen on port 3000
const PORT = process.env.PORT || 3000;

// Configure CORS: Allows your frontend (running on localhost:3001) to make requests to this backend.
// NOTE: Ensure this origin matches your frontend's development server URL.
app.use(cors({
  origin: ['http://localhost:3001'],
  methods: ['GET', 'POST', 'PUT', 'DELETE'],
  allowedHeaders: ['Content-Type', 'Authorization'],
}));

// Middleware to parse JSON request bodies
app.use(express.json({ limit: '50mb' })); // Increased payload limit to handle large JSON data

// --- Local File Storage Setup (for Evidence Uploads) ---

// Create the 'uploads' directory if it doesn't exist
const uploadsDir = path.join(__dirname, 'uploads');
if (!fs.existsSync(uploadsDir)) {
  fs.mkdirSync(uploadsDir);
}

// Configure Multer for file uploads
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    // Files will be saved in the 'uploads' directory within your backend folder
    cb(null, uploadsDir);
  },
  filename: (req, file, cb) => {
    // Generate a unique filename to prevent conflicts
    // Example: originalname-timestamp.ext
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
    cb(null, file.fieldname + '-' + uniqueSuffix + path.extname(file.originalname));
  }
});
const upload = multer({ storage: storage });

// Serve static files from the 'uploads' directory
// This allows the frontend to access uploaded files via URLs like http://localhost:3000/uploads/filename.png
const frameworkDocsDir = path.join(__dirname, 'framework_docs'); // This points to your new folder
if (!fs.existsSync(frameworkDocsDir)) {
  fs.mkdirSync(frameworkDocsDir); // Ensures the directory exists
}
// This line makes files in 'framework_docs' available under the '/framework_docs' URL path
app.use('/framework_docs', express.static(frameworkDocsDir));
console.log(`[Backend Init] Serving framework documents from: ${frameworkDocsDir}`);


// --- Helper function to get current timestamp in Firestore Timestamp format ---
const getTimestamp = () => admin.firestore.Timestamp.now();

// --- API Endpoints ---

// Health Check Endpoint
app.get('/api/health', (req, res) => {
  res.status(200).json({ status: 'Backend API is healthy!', timestamp: new Date().toISOString() });
});

// --- Frameworks Endpoints ---

// GET all frameworks
app.get('/api/frameworks', async (req, res) => {
  try {
    const frameworksRef = db.collection('frameworks');
    const snapshot = await frameworksRef.get();
    const frameworks = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    res.status(200).json(frameworks);
  } catch (error) {
    console.error('[Backend Error] Error getting frameworks:', error);
    res.status(500).send('Error retrieving frameworks: ' + error.message);
  }
});

// GET controls for a specific framework type (e.g., 'Cloud', 'ISMS', 'AI')
app.get('/api/frameworks/:type/controls', async (req, res) => {
  try {
    const { type } = req.params;

    // Step 1: Find all framework IDs that belong to the requested type/domain
    const frameworksQuerySnapshot = await db.collection('frameworks').where('type', '==', type).get();
    const frameworkIds = frameworksQuerySnapshot.docs.map(doc => doc.id);

    if (frameworkIds.length === 0) {
      console.log(`[Backend Debug] No frameworks found for type: ${type}. Returning empty controls list.`);
      return res.status(200).json([]); // No frameworks for this type, so no controls
    }

    // Step 2: Fetch all controls whose framework_id is in the list of found frameworkIds
    // Firestore 'in' query supports up to 10 comparison values
    const controlsQuerySnapshot = await db.collection('controls').where('framework_id', 'in', frameworkIds).get();
    const controls = controlsQuerySnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));

    res.status(200).json(controls);
  } catch (error) {
    console.error(`[Backend Error] Error getting controls for type ${req.params.type}:`, error);
    res.status(500).send('Error retrieving controls: ' + error.message);
  }
});

// GET a single control definition by its ID
app.get('/api/frameworks/control/:controlId', async (req, res) => {
  try {
    const { controlId } = req.params;
    const controlDoc = await db.collection('controls').doc(controlId).get();

    if (!controlDoc.exists) {
      console.log(`[Backend Debug] Control definition with ID ${controlId} not found.`);
      return res.status(404).send('Control not found.');
    }
    const controlData = { id: controlDoc.id, ...controlDoc.data() };
    console.log(`\n[Backend Debug] === START Control Definition for ${controlId} ===`);
    console.log(`[Backend Debug] Control ID: ${controlData.id}`);
    console.log(`[Backend Debug] Control Objective: ${controlData.control_objective}`);
    console.log(`[Backend Debug] Questionnaires received: ${controlData.questionnaires ? controlData.questionnaires.length : 0}`);
    if (controlData.questionnaires && controlData.questionnaires.length > 0) {
        controlData.questionnaires.forEach((q, idx) => {
            console.log(`  [Backend Debug] Q${idx} Text: "${q.question_text}"`);
            console.log(`  [Backend Debug] Q${idx} Options:`, JSON.stringify(q.options, null, 2)); // Log options structure
            if (typeof q.options !== 'object' || Array.isArray(q.options)) {
                console.warn(`  [Backend Warning] Q${idx} options are not a plain object:`, typeof q.options, Array.isArray(q.options) ? 'Array' : '');
            }
        });
    }
    console.log(`[Backend Debug] === END Control Definition for ${controlId} ===\n`);
    res.status(200).json(controlData);
  } catch (error) {
    console.error('[Backend Error] Error getting single control definition:', error);
    res.status(500).send('Error retrieving control definition: ' + error.message);
  }
});

// --- Audits Endpoints ---

// UPDATED LOGIC FOR CREATING A NEW AUDIT (NOW ACCEPTS domain_type AND CLIENT DETAILS)
app.post('/api/audits', async (req, res) => {
  try {
    console.log('[Backend Debug] Received audit creation request. Body:', req.body);
    const {
      title,
      description,
      domain_type,
      userId,
      client_company_name,
      client_spoc_name,
      client_spoc_email,
      client_spoc_phone
    } = req.body;

    if (!title || !domain_type || !userId || !client_company_name) {
      console.error('[Backend Error] Audit creation validation failed: Missing required fields (title, domain_type, userId, client_company_name).', { title, domain_type, userId, client_company_name });
      return res.status(400).send('Title, domain type, user ID, and client company name are required.');
    }

    const frameworksQuerySnapshot = await db.collection('frameworks').where('type', '==', domain_type).get();
    const frameworksAuditedIds = frameworksQuerySnapshot.docs.map(doc => doc.id);

    if (frameworksAuditedIds.length === 0) {
      console.error(`[Backend Error] Audit creation failed: No frameworks found for domain type: ${domain_type}.`);
      return res.status(404).send(`No frameworks found for domain type: ${domain_type}. Cannot create audit.`);
    }

    let allControlsForDomain = [];
    const chunkSize = 10;
    for (let i = 0; i < frameworksAuditedIds.length; i += chunkSize) {
      const chunk = frameworksAuditedIds.slice(i, i + chunkSize);
      const controlsQuerySnapshot = await db.collection('controls').where('framework_id', 'in', chunk).get();
      allControlsForDomain = allControlsForDomain.concat(controlsQuerySnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() })));
    }

    const newAudit = {
      title,
      description: description || '',
      user_id: userId,
      domain_type: domain_type,
      frameworks_audited: frameworksAuditedIds,
      overall_status: 'Not Started',
      overall_score: 0.00,
      created_at: getTimestamp(),
      updated_at: getTimestamp(),
      total_controls_in_audit: allControlsForDomain.length,
      completed_controls_in_audit: 0,
      client_company_name: client_company_name,
      client_spoc_name: client_spoc_name || null,
      client_spoc_email: client_spoc_email || null,
      client_spoc_phone: client_spoc_phone || null
    };

    const auditDocRef = await db.collection('audits').add(newAudit);

    const batch = db.batch();
    console.log(`\n[Backend Debug] === START Initializing Responses for New Audit ${auditDocRef.id} ===`);
    allControlsForDomain.forEach(control => {
      const responseDocRef = auditDocRef.collection('responses').doc(control.id);
      const initialResponseData = {
        control_id: control.id,
        question_responses: (control.questionnaires || []).map((q, index) => ({ // Initialize question responses based on control definition
            question_index: index,
            question_text: q.question_text,
            selected_option: null,
            option_text: null,
        })),
        compliance_status: 'Not Answered',
        justification_text: null,
        maturity_level_selected: null,
        evidence_path: null,
        evidence_filename: null,
        ai_recommendation: null,
        response_date: null,
      };
      batch.set(responseDocRef, initialResponseData);
      console.log(`[Backend Debug]   Control ${control.id} initialized with ${initialResponseData.question_responses.length} questions.`);
    });
    await batch.commit();
    console.log(`[Backend Debug] === END Initializing Responses for New Audit ${auditDocRef.id} ===\n`);

    res.status(201).json({ id: auditDocRef.id, ...newAudit });
  } catch (error) {
    console.error('[Backend Error] Error creating audit:', error);
    res.status(500).send('Error creating audit: ' + error.message);
  }
});

// GET: Get all audits for a specific user
app.get('/api/audits', async (req, res) => {
  try {
    const userId = req.query.userId;

    if (!userId) {
      return res.status(400).send('User ID is required to fetch audits.');
    }

    const auditsRef = db.collection('audits').where('user_id', '==', userId);
    const snapshot = await auditsRef.orderBy('created_at', 'desc').get();
    const audits = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    res.status(200).json(audits);
  } catch (error) {
    console.error('[Backend Error] Error getting audits for user:', error);
    res.status(500).send('Error retrieving audits: ' + error.message);
  }
});

// GET: Get a single audit by ID (with its responses)
app.get('/api/audits/:id', async (req, res) => {
  try {
    const auditId = req.params.id;
    const auditDocRef = db.collection('audits').doc(auditId);
    const auditDoc = await auditDocRef.get();

    if (!auditDoc.exists) {
      return res.status(404).send('Audit not found');
    }

    const auditData = { id: auditDoc.id, ...auditDoc.data() };

    const frameworksAuditedIds = auditData.frameworks_audited || [];
    let allControlsForAudit = [];
    if (frameworksAuditedIds.length > 0) {
      const chunkSize = 10;
      for (let i = 0; i < frameworksAuditedIds.length; i += chunkSize) {
        const chunk = frameworksAuditedIds.slice(i, i + chunkSize);
        const controlsQuerySnapshot = await db.collection('controls').where('framework_id', 'in', chunk).get();
        allControlsForAudit = allControlsForAudit.concat(controlsQuerySnapshot.docs.map(doc => ({ id: doc.id, data: doc.data() })));
      }
    }
    const controlDefinitionsMap = new Map(allControlsForAudit.map(c => [c.id, c.data]));
    const totalExpectedControls = allControlsForAudit.length;


    const responsesRef = auditDocRef.collection('responses');
    const responsesSnapshot = await responsesRef.get();

    let completedControlsCount = 0;
    auditData.responses = responsesSnapshot.docs.map(doc => {
      const responseData = { id: doc.id, ...doc.data() };
      const controlId = responseData.control_id;
      const controlDefinition = controlDefinitionsMap.get(controlId);

      if (controlDefinition) {
        const numQuestions = controlDefinition.questionnaires?.length || 0;
        const allQuestionsAnswered = numQuestions > 0 &&
          responseData.question_responses &&
          responseData.question_responses.filter(qr => qr.selected_option).length === numQuestions;
        const overallStatusSet = ['Yes', 'Partial', 'No', 'Not Applicable'].includes(responseData.compliance_status);

        if (allQuestionsAnswered && overallStatusSet) {
          completedControlsCount++;
        }
      }
      return responseData;
    });

    const overallProgress = totalExpectedControls === 0 ? 0 : Math.round((completedControlsCount / totalExpectedControls) * 100);
    const overallStatus = overallProgress === 100 ? 'Completed' : (overallProgress > 0 ? 'In Progress' : 'Not Started');

    if (auditData.overall_score !== overallProgress || auditData.overall_status !== overallStatus || auditData.completed_controls_in_audit !== completedControlsCount) {
      const updateBatch = db.batch();
      updateBatch.update(auditDocRef, {
        overall_score: overallProgress,
        overall_status: overallStatus,
        completed_controls_in_audit: completedControlsCount,
        updated_at: getTimestamp(),
      });
      await updateBatch.commit();
      auditData.overall_score = overallProgress;
      auditData.overall_status = overallStatus;
      auditData.completed_controls_in_audit = completedControlsCount;
    }


    res.status(200).json(auditData);
  } catch (error) {
    console.error('[Backend Error] Error getting audit by ID:', error);
    res.status(500).send('Error retrieving audit: ' + error.message);
  }
});

// GET a single control's response within an audit
app.get('/api/audits/:auditId/responses/:controlId', async (req, res) => {
    try {
        const { auditId, controlId } = req.params;
        const responseDocRef = db.collection('audits').doc(auditId).collection('responses').doc(controlId);
        const responseDoc = await responseDocRef.get();

        if (!responseDoc.exists) {
            console.log(`[Backend Debug] No existing response found for audit ${auditId} and control ${controlId}. Returning empty.`);
            // Fetch the control definition to provide its initial questionnaires for the response structure
            const controlDefDoc = await db.collection('controls').doc(controlId).get();
            const controlDefData = controlDefDoc.exists ? controlDefDoc.data() : {};
            const initialQuestionResponses = (controlDefData.questionnaires || []).map((q, index) => ({
                question_index: index,
                question_text: q.question_text,
                selected_option: null,
                option_text: null,
            }));

            return res.status(200).json({
                control_id: controlId,
                question_responses: initialQuestionResponses, // Initialize with questions from definition
                compliance_status: 'Not Answered',
                justification_text: '',
                maturity_level_selected: null,
                evidence_path: null,
                evidence_filename: null,
                ai_recommendation: null,
            });
        }
        const responseData = { id: responseDoc.id, ...responseDoc.data() };
        console.log(`\n[Backend Debug] === START Response for Audit ${auditId}, Control ${controlId} ===`);
        console.log(`[Backend Debug] Control ID: ${responseData.control_id}`);
        console.log(`[Backend Debug] Question Responses:`, JSON.stringify(responseData.question_responses, null, 2)); // Log question_responses structure
        console.log(`[Backend Debug] Compliance Status: ${responseData.compliance_status}`);
        console.log(`[Backend Debug] === END Response for Audit ${auditId}, Control ${controlId} ===\n`);
        res.status(200).json(responseData);
    } catch (error) {
        console.error('[Backend Error] Error getting single audit control response:', error);
        res.status(500).send('Error retrieving control response: ' + error.message);
    }
});


// PUT: Update audit responses for a specific control within an audit
app.put('/api/audits/:id/responses', async (req, res) => {
  try {
    const auditId = req.params.id;
    const { control_id, question_responses, compliance_status, justification_text, maturity_level_selected, evidence_path, evidence_filename, ai_recommendation } = req.body;

    console.log(`[Backend Debug] PUT /api/audits/${auditId}/responses called for control ${control_id}`);
    console.log(`[Backend Debug] Received question_responses:`, JSON.stringify(question_responses, null, 2));


    if (!control_id || !Array.isArray(question_responses)) {
      console.error('[Backend Error] Audit response update failed: Missing control_id or question_responses array.');
      return res.status(400).send('Control ID and question responses array are required.');
    }

    const auditDocRef = db.collection('audits').doc(auditId);
    const responseDocRef = auditDocRef.collection('responses').doc(control_id);
    const batch = db.batch();

    // Save/Update the specific control response
    batch.set(responseDocRef, {
      control_id,
      question_responses: question_responses,
      compliance_status: compliance_status || 'Not Answered',
      justification_text: justification_text || null,
      maturity_level_selected: maturity_level_selected || null,
      evidence_path: evidence_path || null,
      evidence_filename: evidence_filename || null,
      ai_recommendation: ai_recommendation || null,
      response_date: getTimestamp(),
    }, { merge: true }); // Merge to update specific fields

    const auditDoc = await auditDocRef.get();
    if (!auditDoc.exists) {
      console.error('[Backend Error] Audit not found during response update for audit ID:', auditId);
      return res.status(404).send('Audit not found during response update.');
    }
    const auditData = auditDoc.data();
    const totalExpectedControls = auditData.total_controls_in_audit || 0;

    const controlDefinitionDoc = await db.collection('controls').doc(control_id).get();
    const controlDefinition = controlDefinitionDoc.data();
    const numQuestions = controlDefinition?.questionnaires?.length || 0;

    const allQuestionsAnswered = numQuestions > 0 &&
      question_responses.filter(qr => qr.selected_option).length === numQuestions;
    const overallStatusSet = ['Yes', 'Partial', 'No', 'Not Applicable'].includes(compliance_status);

    const allResponsesSnapshot = await auditDocRef.collection('responses').get();
    let currentCompletedControlsCount = 0;

    let allControlsForAudit = [];
    const frameworksInAudit = auditData.frameworks_audited || [];
    if (frameworksInAudit.length > 0) {
      const chunkSize = 10;
      for (let i = 0; i < frameworksInAudit.length; i += chunkSize) {
        const chunk = frameworksInAudit.slice(i, i + chunkSize);
        const controlsQuerySnapshot = await db.collection('controls').where('framework_id', 'in', chunk).get();
        allControlsForAudit = allControlsForAudit.concat(controlsQuerySnapshot.docs.map(doc => ({ id: doc.id, data: doc.data() })));
      }
    }
    const currentControlDefinitionsMap = new Map(allControlsForAudit.map(c => [c.id, c.data]));


    allResponsesSnapshot.docs.forEach(responseDoc => {
      const resData = responseDoc.data();
      const resControlId = resData.control_id;
      const resControlDef = currentControlDefinitionsMap.get(resControlId);

      if (resControlDef) {
        const resNumQuestions = resControlDef.questionnaires?.length || 0;
        const resAllQuestionsAnswered = resNumQuestions > 0 &&
          resData.question_responses &&
          resData.question_responses.filter(qr => qr.selected_option).length === resNumQuestions;
        const resOverallStatusSet = ['Yes', 'Partial', 'No', 'Not Applicable'].includes(resData.compliance_status);

        if (resAllQuestionsAnswered && resOverallStatusSet) {
          currentCompletedControlsCount++;
        }
      }
    });


    const newOverallProgress = totalExpectedControls === 0 ? 0 : Math.round((currentCompletedControlsCount / totalExpectedControls) * 100);
    const newOverallStatus = newOverallProgress === 100 ? 'Completed' : (newOverallProgress > 0 ? 'In Progress' : 'Not Started');

    batch.update(auditDocRef, {
      overall_score: newOverallProgress,
      overall_status: newOverallStatus,
      completed_controls_in_audit: currentCompletedControlsCount,
      updated_at: getTimestamp(),
    });

    await batch.commit();
    res.status(200).json({ message: 'Audit response and overall progress updated successfully', newOverallProgress, newOverallStatus });
  } catch (error) {
    console.error('[Backend Error] Error updating audit response and overall progress:', error);
    res.status(500).send('Error updating audit response and overall progress: ' + error.message);
  }
});


// --- File Upload Endpoint (Local Disk Storage) ---

app.post('/api/upload-evidence', upload.single('evidenceFile'), (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).send('No file uploaded.');
    }

    const filePath = `/uploads/${req.file.filename}`;
    console.log(`[Backend Debug] File uploaded locally: ${filePath}`);

    res.status(200).json({
      message: 'File uploaded successfully',
      evidence_path: filePath,
      evidence_filename: req.file.originalname,
    });
  } catch (error) {
    console.error('[Backend Error] Error uploading file:', error);
    res.status(500).send('Error uploading file: ' + error.message);
  }
});

// --- AI Recommendation Endpoint (Gemini API) ---

let genAIInstance; // Renamed to avoid confusion with `genAI` at global scope
try {
  const geminiApiKey = process.env.GEMINI_API_KEY;
  if (!geminiApiKey) {
    throw new Error('GEMINI_API_KEY is not set in .env file. Gemini API will not be available.');
  }
  genAIInstance = new GoogleGenerativeAI(geminiApiKey); // Use new instance variable
  console.log('[Backend Init] Google Gemini API initialized.');
} catch (e) {
  console.error(e.message);
  genAIInstance = null; // Set to null if initialization fails
}


app.post('/api/generate-recommendation', async (req, res) => {
  try {
    if (!genAIInstance) { // Use the new instance variable
      return res.status(503).send('AI service is not available (API key missing or initialization failed).');
    }

    const { control_objective, audit_question, compliance_status, justification_text } = req.body;

    if (!control_objective || !audit_question || !compliance_status) {
      return res.status(400).send('Control objective, audit question, and compliance status are required for recommendation.');
    }

    const prompt = `Given the following security control:
Control Objective: "${control_objective}"
Audit Question: "${audit_question}"
Current Compliance Status: "${compliance_status}"
Justification (if any): "${justification_text || 'None provided'}"

Please provide a concise, actionable recommendation for remediation to achieve or improve compliance. Focus on practical steps and industry best practices. If the status is 'Yes' or 'Not Applicable', you can simply state that no remediation is needed.`;

    const model = genAIInstance.getGenerativeModel({ model: "gemini-2.0-flash" }); // Use new instance variable
    const result = await model.generateContent(prompt);
    const response = await result.response;
    const recommendationText = response.text();

    res.status(200).json({ recommendation: recommendationText });

  } catch (error) {
    console.error('[Backend Error] Error generating AI recommendation:', error);
    if (error.response && error.response.status) {
      return res.status(error.response.status).send(`Gemini API error: ${error.response.statusText || error.message}`);
    }
    res.status(500).send('Error generating AI recommendation: ' + error.message);
  }
});

// --- TEMPORARY BULK UPLOAD ENDPOINT FOR CONTROLS ---
app.post('/api/seed-controls', async (req, res) => {
  try {
    const controlsData = req.body;

    if (!Array.isArray(controlsData) || controlsData.length === 0) {
      return res.status(400).send('Request body must be a non-empty array of control objects.');
    }

    const batch = db.batch();
    const collectionRef = db.collection('controls');

    for (const control of controlsData) {
      if (!control.id) {
        console.warn('Skipping control with no ID:', control);
        continue;
      }
      const docRef = collectionRef.doc(control.id);
      batch.set(docRef, {
        ...control,
        created_at: getTimestamp(),
        updated_at: getTimestamp()
      }, { merge: false });
    }

    await batch.commit();
    res.status(201).json({ message: `Successfully seeded ${controlsData.length} controls to Firestore.`, seededCount: controlsData.length });
  } catch (error) {
    console.error('[Backend Error] Error seeding controls:', error);
    res.status(500).send('Error seeding controls: ' + error.message);
  }
});


// NEW ENDPOINT: Generate PDF Report
app.get('/api/audits/:auditId/report/pdf', async (req, res) => {
    try {
        const { auditId } = req.params;
        const auditDocRef = db.collection('audits').doc(auditId);
        const auditDoc = await auditDocRef.get();

        if (!auditDoc.exists) {
            console.error(`[Backend ERROR] PDF Report: Audit with ID ${auditId} not found.`);
            return res.status(404).send('Audit not found.');
        }

        const auditData = { id: auditDoc.id, ...auditDoc.data() };
        console.log(`[Backend DEBUG] PDF Report: Audit Data Fetched:`, auditData.title);

        const frameworksAuditedIds = auditData.frameworks_audited || [];
        let allControlsForAudit = [];
        if (frameworksAuditedIds.length > 0) {
            const chunkSize = 10; // Batching 'in' queries
            for (let i = 0; i < frameworksAuditedIds.length; i += chunkSize) {
                const chunk = frameworksAuditedIds.slice(i, i + chunkSize);
                const controlsQuerySnapshot = await db.collection('controls').where('framework_id', 'in', chunk).get();
                allControlsForAudit = allControlsForAudit.concat(controlsQuerySnapshot.docs.map(doc => ({ id: doc.id, data: doc.data() })));
            }
        }
        const controlDefinitionsMap = new Map(allControlsForAudit.map(c => [c.id, c.data]));
        console.log(`[Backend DEBUG] PDF Report: Fetched ${controlDefinitionsMap.size} control definitions related to audited frameworks.`);


        const responsesRef = auditDocRef.collection('responses');
        const responsesSnapshot = await responsesRef.get();
        const auditResponses = {}; // Using an object for easier lookup by control_id
        responsesSnapshot.docs.forEach(doc => {
            auditResponses[doc.id] = doc.data();
        });
        console.log(`[Backend DEBUG] PDF Report: Fetched ${Object.keys(auditResponses).length} audit responses.`);
        
        // Filter controls to report on to only include those defined in the audit's frameworks
        const controlsToReport = Object.values(controlDefinitionsMap).filter(controlDef => 
            auditResponses[controlDef.id] // Only include controls that have a response
        ).sort((a, b) => a.id.localeCompare(b.id)); // Sort by control ID

        console.log(`[Backend DEBUG] PDF Report: ${controlsToReport.length} controls will be included in the report.`);

        // Prepare report data structure
        const reportData = {
            audit: {
                ...auditData,
                created_at: auditData.created_at ? auditData.created_at.toDate().toLocaleDateString() : 'N/A',
                updated_at: auditData.updated_at ? auditData.updated_at.toDate().toLocaleDateString() : 'N/A',
            },
            client: {
                company_name: auditData.client_company_name || 'N/A',
                spoc_name: auditData.client_spoc_name || 'N/A',
                spoc_email: auditData.client_spoc_email || 'N/A',
                spoc_phone: auditData.client_spoc_phone || 'N/A',
            },
            controls: controlsToReport.map(controlDef => {
                const response = auditResponses[controlDef.id] || {}; // Get response for this control
                return {
                    id: controlDef.id,
                    objective: controlDef.control_objective || 'N/A',
                    description: controlDef.control_description || 'N/A',
                    compliance_status: response.compliance_status || 'Not Answered',
                    maturity_level: response.maturity_level_selected || 'N/A',
                    justification: response.justification_text || 'None provided',
                    evidence_filename: response.evidence_filename || 'N/A',
                    evidence_path: response.evidence_path ? `${req.protocol}://${req.get('host')}${response.evidence_path}` : '#', // Full URL for evidence
                    ai_recommendation: response.ai_recommendation || 'None',
                    questionnaire_answers: (response.question_responses || []).map(qr => ({
                        question_text: qr.question_text,
                        selected_option: qr.option_text || qr.selected_option || 'Not Answered', // Prefer option_text if available
                    }))
                };
            }),
            summary: {
                total: auditData.total_controls_in_audit,
                completed: auditData.completed_controls_in_audit,
                score: auditData.overall_score,
                status: auditData.overall_status,
                yes: auditResponses ? Object.values(auditResponses).filter(r => r.compliance_status === 'Yes').length : 0,
                partial: auditResponses ? Object.values(auditResponses).filter(r => r.compliance_status === 'Partial').length : 0,
                no: auditResponses ? Object.values(auditResponses).filter(r => r.compliance_status === 'No').length : 0,
                notApplicable: auditResponses ? Object.values(auditResponses).filter(r => r.compliance_status === 'Not Applicable').length : 0,
                notAnswered: auditResponses ? Object.values(auditResponses).filter(r => r.compliance_status === 'Not Answered').length : 0,
            }
        };
        console.log(`[Backend DEBUG] PDF Report: Report Data Summary:`, JSON.stringify(reportData.summary, null, 2));


        const doc = new PDFDocument();
        let buffers = [];
        doc.on('data', buffers.push.bind(buffers));
        doc.on('end', () => {
            let pdfBuffer = Buffer.concat(buffers);
            res.setHeader('Content-Type', 'application/pdf');
            res.setHeader('Content-Disposition', `attachment; filename=audit_report_${auditId}.pdf`);
            res.send(pdfBuffer);
            console.log(`[Backend DEBUG] PDF Report: PDF sent successfully for audit ID: ${auditId}.`);
        });

        // PDF Content Generation
        doc.fontSize(25).text(`Audit Report: ${reportData.audit.title}`, { align: 'center' });
        doc.moveDown();
        doc.fontSize(12).text(`Client: ${reportData.client.company_name}`);
        doc.text(`Framework: ${reportData.audit.domain_type}`);
        doc.text(`Status: ${reportData.audit.overall_status}`);
        doc.text(`Score: ${reportData.audit.overall_score}%`);
        doc.moveDown();

        doc.fontSize(16).text('Client Details', { underline: true });
        doc.moveDown(0.5);
        doc.fontSize(12).text(`Company Name: ${reportData.client.company_name}`);
        doc.text(`SPOC Name: ${reportData.client.spoc_name}`);
        doc.text(`SPOC Email: ${reportData.client.spoc_email}`);
        doc.text(`SPOC Phone: ${reportData.client.spoc_phone}`);
        doc.moveDown();

        doc.fontSize(16).text('Audit Summary', { underline: true });
        doc.moveDown(0.5);
        doc.fontSize(12).text(`Total Controls: ${reportData.summary.total}`);
        doc.text(`Completed Controls: ${reportData.summary.completed}`);
        doc.text(`Overall Score: ${reportData.summary.score}%`);
        doc.text(`Compliance Breakdown: Yes (${reportData.summary.yes}), Partial (${reportData.summary.partial}), No (${reportData.summary.no}), N/A (${reportData.summary.notApplicable}), Not Answered (${reportData.summary.notAnswered})`);
        doc.moveDown();

        doc.addPage(); // Start controls on a new page
        doc.fontSize(18).text('Detailed Control Responses', { align: 'center', underline: true });
        doc.moveDown();

        if (reportData.controls.length === 0) {
            doc.fontSize(12).text('No control responses to display for this audit.', { align: 'center' });
        } else {
            reportData.controls.forEach((control, index) => {
                doc.fontSize(14).text(`${control.id}: ${control.objective}`, { underline: true });
                doc.moveDown(0.2);
                doc.fontSize(10).text(`Description: ${control.description}`);
                doc.text(`Compliance Status: ${control.compliance_status}`);
                doc.text(`Maturity Level: ${control.maturity_level}`);
                doc.text(`Justification: ${control.justification}`);
                if (control.evidence_path !== '#') {
                    doc.text(`Evidence: ${control.evidence_filename} (${control.evidence_path})`);
                }
                if (control.ai_recommendation !== 'None') {
                    doc.text(`AI Recommendation: ${control.ai_recommendation}`);
                }
                doc.moveDown(0.5);

                if (control.questionnaire_answers && control.questionnaire_answers.length > 0) {
                    doc.fontSize(10).text('Questionnaire Answers:');
                    control.questionnaire_answers.forEach((qa, qIdx) => {
                        doc.text(`  Q${qIdx + 1}: ${qa.question_text}`);
                        doc.text(`    Selected Option: ${qa.selected_option}`);
                    });
                }
                doc.moveDown();
                if (index < reportData.controls.length - 1) {
                    doc.moveDown(0.5); // Add some space between controls
                }
            });
        }
        
        doc.end();

    } catch (error) {
        console.error('[Backend Error] Error generating PDF report:', error);
        res.status(500).send('Failed to generate PDF report: ' + error.message);
    }
});


// Excel Report Route (UPDATED WITH MORE DEBUGGING)
app.get('/api/audits/:auditId/checklist/xlsx', async (req, res) => {
    const auditId = req.params.auditId;
    console.log(`[Backend DEBUG] --- START Generating Excel checklist for audit ID: ${auditId} ---`);

    try {
        // 1. Fetch Audit Details
        const auditDoc = await db.collection('audits').doc(auditId).get();
        if (!auditDoc.exists) {
            console.error(`[Backend ERROR] Excel Report: Audit with ID ${auditId} not found.`);
            return res.status(404).send('Audit not found.');
        }
        const auditData = auditDoc.data();
        console.log(`[Backend DEBUG] Excel Report: Audit Data Fetched:`, auditData.title);
        console.log(`[Backend DEBUG] Excel Report: Audit Frameworks:`, auditData.frameworks_audited);


        // 2. Fetch All Control Definitions for the frameworks audited in this audit
        const frameworksAuditedIds = auditData.frameworks_audited || [];
        let allControlsForAudit = [];
        if (frameworksAuditedIds.length > 0) {
            const chunkSize = 10; // Firestore 'in' query limit
            for (let i = 0; i < frameworksAuditedIds.length; i += chunkSize) {
                const chunk = frameworksAuditedIds.slice(i, i + chunkSize);
                const controlsQuerySnapshot = await db.collection('controls').where('framework_id', 'in', chunk).get();
                allControlsForAudit = allControlsForAudit.concat(controlsQuerySnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() })));
            }
        }
        const controlDefinitionsMap = new Map(allControlsForAudit.map(c => [c.id, c]));
        console.log(`[Backend DEBUG] Excel Report: Fetched ${allControlsForAudit.length} control definitions related to audited frameworks.`);


        // 3. Fetch All Audit Responses for this Audit from the subcollection
        const responsesSnapshot = await db.collection('audits').doc(auditId).collection('responses').get();
        const auditResponses = {}; // Using an object for easier lookup by control_id
        responsesSnapshot.docs.forEach(doc => {
            auditResponses[doc.id] = doc.data();
        });
        console.log(`[Backend DEBUG] Excel Report: Fetched ${Object.keys(auditResponses).length} audit responses for audit ID: ${auditId}.`);
        console.log(`[Backend DEBUG] Excel Report: Audit Responses Content (first few):`, JSON.stringify(Object.values(auditResponses).slice(0, 3), null, 2));


        // Create a new workbook and a worksheet
        const workbook = new ExcelJS.Workbook();
        const worksheet = workbook.addWorksheet(`Audit Checklist - ${auditData.title}`);

        // Define columns for the worksheet
        worksheet.columns = [
            { header: 'Control ID', key: 'controlId', width: 15 },
            { header: 'Control Objective', key: 'controlObjective', width: 40 },
            { header: 'Question', key: 'question', width: 60 },
            { header: 'Selected Option', key: 'selectedOption', width: 25 },
            { header: 'Compliance Status', key: 'complianceStatus', width: 20 },
            { header: 'Justification', key: 'justification', width: 50 },
            { header: 'Maturity Level', key: 'maturityLevel', width: 15 },
            { header: 'AI Recommendation', key: 'aiRecommendation', width: 60 },
            { header: 'Evidence Filename', key: 'evidenceFilename', width: 30 },
            { header: 'Evidence Link', key: 'evidenceLink', width: 50 },
        ];

        // Apply some basic styling for headers
        worksheet.getRow(1).eachCell((cell) => {
            cell.font = { bold: true, color: { argb: 'FFFFFFFF' } }; // White font
            cell.fill = {
                type: 'pattern',
                pattern: 'solid',
                fgColor: { argb: 'FF4F46E5' } // Tailwind purple-700 approx
            };
            cell.alignment = { vertical: 'middle', horizontal: 'center' };
            cell.border = {
                top: { style: 'thin' },
                left: { style: 'thin' },
                bottom: { style: 'thin' },
                right: { style: 'thin' }
            };
        });

        // Prepare data rows: Iterate over controls *defined for the audit's framework*, then find responses.
        // This ensures all expected controls are listed, even if no response has been saved yet.
        const controlsForExcel = allControlsForAudit.sort((a, b) => a.id.localeCompare(b.id)); // Sort by ID

        if (controlsForExcel.length === 0) {
            console.warn(`[Backend WARNING] Excel Report: No control definitions found for frameworks audited in audit ID: ${auditId}.`);
            // Add a message row to the Excel if no data
            worksheet.addRow({
                controlId: 'N/A',
                controlObjective: 'No controls found for the frameworks associated with this audit.',
                question: '', selectedOption: '', complianceStatus: '', justification: '',
                maturityLevel: '', aiRecommendation: '', evidenceFilename: '', evidenceLink: ''
            });
        }

        for (const control of controlsForExcel) {
            const response = auditResponses[control.id]; // Get response for this control (might be undefined)
            console.log(`[Backend DEBUG] Excel Report: Processing Control: ${control.id}, Response Found: ${!!response}`);

            // Ensure control.questionnaires is an array before iterating
            (control.questionnaires || []).forEach((q, qIndex) => {
                const questionResponse = response?.question_responses?.[qIndex]; // Get specific question response
                console.log(`  [Backend DEBUG] Excel Report: Q${qIndex} for ${control.id}: Question Text: "${q.question_text}", Selected Option: "${questionResponse?.selected_option || 'N/A'}"`);

                const rowData = {
                    controlId: qIndex === 0 ? control.id : '', // Only show Control ID once per control block
                    controlObjective: qIndex === 0 ? control.control_objective : '', // Only show Objective once
                    question: q.question_text,
                    selectedOption: questionResponse?.option_text || questionResponse?.selected_option || 'Not Answered', // Prefer option_text
                    complianceStatus: qIndex === 0 ? response?.compliance_status || 'Not Answered' : '', // Only show status once
                    justification: qIndex === 0 ? response?.justification_text || 'None Provided' : '', // Only show justification once
                    maturityLevel: qIndex === 0 ? response?.maturity_level_selected || 'N/A' : '', // Only show maturity once
                    aiRecommendation: qIndex === 0 ? response?.ai_recommendation || 'None' : '', // Only show AI recommendation once
                    evidenceFilename: qIndex === 0 ? response?.evidence_filename || 'N/A' : '', // Only show filename once
                    evidenceLink: qIndex === 0 && response?.evidence_path ? `${req.protocol}://${req.get('host')}${response.evidence_path}` : 'N/A', // Only show link once
                };
                worksheet.addRow(rowData);
            });

            // Add an empty row for separation after each control's questions, but not after the very last control
            if ((control.questionnaires?.length || 0) > 0 && controlsForExcel.indexOf(control) < controlsForExcel.length - 1) { 
                worksheet.addRow({}); // Add a blank row
            }
        }
        
        // Set response headers for file download
        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.setHeader('Content-Disposition', `attachment; filename=audit_checklist_${auditId}.xlsx`);

        // Send the workbook to the response
        await workbook.xlsx.write(res);
        res.end(); // End the response stream

        console.log(`[Backend DEBUG] --- END Successfully generated and sent Excel checklist for audit ID: ${auditId} ---`);

    } catch (error) {
        console.error(`[Backend ERROR] Failed to generate Excel checklist for audit ID ${auditId}:`, error);
        res.status(500).send('Failed to generate Excel checklist: ' + error.message);
    }
});


// Start the Express Server
app.listen(PORT, () => {
  console.log(`SecUrAuditz Backend listening on port ${PORT}`);
  console.log(`Access health check at: http://localhost:${PORT}/api/health`);
  console.log(`File uploads stored in: ${uploadsDir}`);
  console.log(`TEMPORARY: Use POST http://localhost:${PORT}/api/seed-controls to upload control data.`);
});