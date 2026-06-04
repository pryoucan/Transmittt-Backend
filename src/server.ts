import express from "express";
import dotenv from "dotenv";
import postgres from 'postgres'
import multer from "multer";
import cors from "cors";
import { createClient } from "@supabase/supabase-js";
import ws from "ws";
import nodeCron from "node-cron";

const app = express();

dotenv.config();

const PORT =  Number(process.env.PORT) || 5009;

// Database configuration
const connectionString = process.env.DATABASE_URL

if (!connectionString) {
    throw new Error("DATABASE_URL is missing");
}

if (!process.env.SUPABASE_URL) {
    throw new Error("SUPABASE_URL is missing");
}

if (!process.env.SUPABASE_SECRET_KEY) {
    throw new Error("SUPABASE_SECRET_KEY is missing");
}

const sql = postgres(connectionString);

// S3 Bucket Configuration
(global as any).WebSocket = ws;
const supabase = createClient(`${process.env.SUPABASE_URL}`, `${process.env.SUPABASE_SECRET_KEY}`);

// Allowed frontend origins
app.use(cors({
    origin: process.env.FRONTEND_URL
}));

app.use(express.json({ limit: '150mb' }));

// Health checkpoint API
app.get("/api/health", (req, res) => {
    return res.status(200).json({
        success: true,
        message: "Server is healthy"
    });
});

// Scheduler function for removing stale/old files
nodeCron.schedule("0 6 * * 1-6", async () => {
    console.log("Cleaning...");
    try {
        const result = await sql `SELECT file_bucket_name FROM file WHERE created_at < NOW() - INTERVAL '.5 day'`;
        console.log(result);
        if(result.length) { 
            let removalOfFiles:Array<string> = [];
            for(let i = 0; i < result.length; i++) {
                removalOfFiles.push(result[i].file_bucket_name);
            }

            deleteSupabaseStorageFile(removalOfFiles);

        }
        await sql `Delete FROM text WHERE created_at < NOW() - INTERVAL '.5 day'`;
        console.log("Cleanup service completed!");
    }
    catch(error) {
        console.log("Cleanup failed", error);
    }
});

// Multer upload using memory-storage middleware
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 15000000
  }
}).single("file");

// Upload route - File
app.post("/api/files/upload", async (req, res) => {
    upload(req, res, async (err) => {
        if(err) {
            console.error(err);
            return res.status(500).json({ error: err });
        }
        if(!req.file) {
            return res.status(400).json({
                success: false,
                message: 'Please send file'
            });
        }
        console.log(req.file);

        console.log("Uploading in S3 Bucket...");

        const bucket_file_name = `${Date.now()}-${req.file.originalname}`;
        const { data, error } = await supabase.storage.from("file")
        .upload(bucket_file_name, req.file.buffer, {
            contentType: req.file.mimetype
        });

        if(error) {
            console.log(error);
            return res.status(400).json({
                success: false,
                message: "File upload to S3 bucket failed"
            });
        }
        else {
            console.log(data);
            const result = await sql
            `
            INSERT INTO file (
            file_name,
            file_format,
            file_size,
            file_bucket_name
            )
            VALUES (
            ${req.file.originalname},
            ${req.file.mimetype},
            ${req.file.size},
            ${bucket_file_name}
            )
            `;

            return res.status(200).json({
                success: true,
                message: "File uploaded successfully"
            });
        }
    });
});

// Delete route - file (supabase storage)
const deleteSupabaseStorageFile = async (filesToBeRemoved:Array<string>) => {
    const { data, error } = await supabase.storage.from("file").remove(filesToBeRemoved);
    if(error) {
        console.log("Supabase storage file deletion error", error);
    }
    else {
        console.log("Supabase storage file deletion success");
        await sql `Delete FROM file WHERE created_at < NOW() - INTERVAL '.5 day'`;
    }
};

// Fetch route - combined files and text
app.get("/api/files", async (req, res) => {
    try {
        const result = await sql`
        SELECT * FROM (
            (SELECT 
                id::text, 
                file_name, 
                file_size::text, 
                created_at, 
                'file' as type
            FROM file
            ORDER BY created_at DESC LIMIT 3)
            UNION ALL
            (SELECT 
                id::text, 
                text_name as file_name, 
                null as file_size, 
                created_at, 
                'text' as type
            FROM text
            ORDER BY created_at DESC LIMIT 3)
        ) AS combined
        ORDER BY created_at DESC 
        LIMIT 3;
        `;
        console.log(`Data fetched successfully (${result.length})`);
        return res.status(200).json({
            success: true,
            message: "Data fetched successfully",
            data: result
        });
    } catch (error) {
        console.error("Fetch files error:", error);
        return res.status(500).json({ success: false, message: "Failed to fetch data" });
    }
});

// Download route - file
app.get("/api/files/download/:file_name", async (req, res) => {
    try {
        const { file_name } = req.params;
        const queryResult = await sql
        `SELECT file_bucket_name FROM file WHERE file_name = ${file_name}`;

        if(!queryResult.length) {
            return res.status(404).json({
                success: false,
                message: "Invalid file"
            });
        }

        console.log(queryResult);
        const { data } = await supabase.storage.from("file").getPublicUrl(queryResult[0].file_bucket_name);
        const result = data.publicUrl;
        console.log(result);

        return res.status(200).json({
            success: true,
            message: "Url found successfully",
            data: result
        });
    }
    catch(error) {
        console.log(error);
        return res.status(500).json({
            success: false,
            message: "Server error",
        });
    }
})

// Upload route - Text
app.post("/api/text/upload", async (req, res) => {
    try {
        const { text } = req.body;
        if (!text) {
            return res.status(400).json({ success: false, message: "Please provide text content" });
        }
        if (text.length > 50000) {
            return res.status(400).json({ success: false, message: "Text exceeds 50,000 characters limit" });
        }
        
        await sql
        `INSERT INTO text (text_name) VALUES (${text})`;
        
        return res.status(200).json({
            success: true,
            message: "Text saved successfully"
        });
    } catch (error) {
        console.error("Text upload error:", error);
        return res.status(500).json({ success: false, message: "Failed to save text" });
    }
});

// View route - Text
app.get("/api/text/:id", async (req, res) => {
    try {
        const { id } = req.params;
        const result = await sql
        `SELECT text_name FROM text WHERE id = ${id}`;
        if (!result.length) {
            return res.status(404).json({ success: false, message: "Text not found" });
        }
        
        return res.status(200).json({
            success: true,
            data: result[0].text_name
        });
    } catch (error) {
        console.error("Fetch text error:", error);
        return res.status(500).json({ success: false, message: "Server error" });
    }
});

app.listen(PORT, "0.0.0.0", async () => {
    console.log(`SERVER IS RUNNING ON PORT ${PORT}`);
    const result = await sql`SELECT now()`;
    console.log(result);
});
