package com.edu;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.rendering.ImageType;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileWriter;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class PdfWorker {

    public static void work(String Action, String fileUrl, String saveDir) throws Exception {
        S3Client s3Client = S3Client.create();

        try {
            // Download the PDF file
            URL url = new URL(fileUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            InputStream inputStream = connection.getInputStream();
            FileOutputStream fileOutputStream = new FileOutputStream(saveDir + ".pdf");
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                fileOutputStream.write(buffer, 0, bytesRead);
            }
            inputStream.close();
            fileOutputStream.close();
            connection.disconnect();
            System.out.println("Downloaded PDF file to: " + saveDir + ".pdf");
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        File completedFile = null; // To track the generated file for upload

        // Perform the specified action on the first page
        switch (Action) {
            case "toImage":
                try {
                    PDDocument document = PDDocument.load(new File(saveDir + ".pdf"));
                    PDFRenderer pdfRenderer = new PDFRenderer(document);

                    // Render the first page as an image
                    BufferedImage bim = pdfRenderer.renderImageWithDPI(0, 300, ImageType.RGB);
                    completedFile = new File(saveDir + "-page-1.png");
                    ImageIO.write(bim, "png", completedFile);

                    System.out.println("Saved first page as image: " + completedFile.getAbsolutePath());
                    document.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;

            case "toText":
                try {
                    PDDocument document = PDDocument.load(new File(saveDir + ".pdf"));
                    PDFTextStripper textStripper = new PDFTextStripper();
                    textStripper.setStartPage(1);
                    textStripper.setEndPage(1);

                    // Extract text from the first page
                    String text = textStripper.getText(document);
                    completedFile = new File(saveDir + "-page-1.txt");
                    try (FileWriter writer = new FileWriter(completedFile)) {
                        writer.write(text);
                    }

                    System.out.println("Saved text of first page: " + completedFile.getAbsolutePath());
                    document.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;

            case "toHTML":
                try {
                    PDDocument document = PDDocument.load(new File(saveDir + ".pdf"));
                    PDFTextStripper textStripper = new PDFTextStripper();
                    textStripper.setStartPage(1);
                    textStripper.setEndPage(1);

                    // Extract text and create basic HTML for the first page
                    StringBuilder htmlContent = new StringBuilder();
                    htmlContent.append("<!DOCTYPE html><html><head><title>PDF to HTML</title></head><body>");
                    htmlContent.append("<h1>PDF Content - First Page</h1>");
                    htmlContent.append("<div style=\"white-space: pre-wrap;\">");

                    String text = textStripper.getText(document);
                    htmlContent.append(text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"));

                    htmlContent.append("</div></body></html>");

                    completedFile = new File(saveDir + "-page-1.html");
                    try (FileWriter writer = new FileWriter(completedFile)) {
                        writer.write(htmlContent.toString());
                    }

                    System.out.println("Saved HTML of first page: " + completedFile.getAbsolutePath());
                    document.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;

            default:
                System.out.println("Unknown action: " + Action);
                return;
        }

        // Upload the completed file to S3
        if (completedFile != null && completedFile.exists()) {
            try {
                PutObjectRequest putRequest = PutObjectRequest.builder()
                        .bucket("resultbucket-aws1")
                        .key(completedFile.getName())
                        .build();

                s3Client.putObject(putRequest, completedFile.toPath());
                System.out.println("Uploaded file to S3: " + completedFile.getName());
            } catch (S3Exception e) {
                System.err.println("S3 Error: " + e.awsErrorDetails().errorMessage());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("No file was generated to upload.");
        }

        // Close the S3 client
        s3Client.close();
    }
}
