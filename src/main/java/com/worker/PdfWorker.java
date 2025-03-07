package com.edu;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.UUID;


import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.rendering.ImageType;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.UUID;


import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.regions.Region;


public class PdfWorker {

    

        //use httpurlconnection to check if the url returns an error
        //if it does return an error, return the error message
        //if it does not return an error, download the pdf file
        //if the pdf file is downloaded, perform the specified action on the first page
        //if the action is to convert the pdf to an image, render the first page as an image
        //if the action is to convert the pdf to text, extract text from the first page
        //if the action is to convert the pdf to html, extract text and create basic html for the first page
        //upload the completed file to s3
        //delete the pdf file
        //return the result
        

    public static String work(String Action, String fileUrl,String id) throws Exception {
        HttpURLConnection.setFollowRedirects(false);
        try {
        HttpURLConnection con = (HttpURLConnection) new URL(fileUrl).openConnection();
        con.setRequestMethod("HEAD");
        con.setConnectTimeout(10000); // Set timeout for connection
        con.setReadTimeout(10000);    // Set timeout for reading response

        int responseCode = con.getResponseCode();

        if (responseCode == HttpURLConnection.HTTP_OK) {
            System.out.println("URL is valid and exists");
        } else {
            String code = String.valueOf(responseCode);
            System.out.println("Error Code: " + code);
            return "Error:" + code;
        }
    } catch (UnknownHostException e) {
        System.err.println("Domain expired or host not found: " + e.getMessage());
        return "Error:991";
    } catch (SocketTimeoutException e) {
        System.err.println("Connection timed out: " + e.getMessage());
        return "Error:992";
    } catch (IOException e) {
        System.err.println("IO Error: " + e.getMessage());
        return "Error:993";
    }
        S3Client s3Client = S3Client.builder().region(Region.US_EAST_1).build();
        String saveDir = UUID.randomUUID().toString();

        try {
            // Download only the first page of the PDF file
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
            return "Error:994";
        }
        File completedFile = null; // To track the generated file for upload

        //check if pdf file is not empty
        if (new File(saveDir + ".pdf").length() == 0) {
            return "Error:994";
        }
        try (PDDocument document = PDDocument.load(new File(saveDir + ".pdf"))) {
            // Process the document
        } catch (IOException e) {
            System.err.println("Failed to load PDF: " + e.getMessage());
            return "Error:994";
        }
        switch (Action) {
            case "ToImage":
                try {
                    PDDocument document = PDDocument.load(new File(saveDir + ".pdf"));
                    PDFRenderer pdfRenderer = new PDFRenderer(document);

                    // Render the first page as an image
                    BufferedImage bim = pdfRenderer.renderImageWithDPI(0, 300, ImageType.RGB);
                    completedFile = new File(saveDir + ".png");
                    ImageIO.write(bim, "png", completedFile);

                    System.out.println("Saved first page as image: " + completedFile.getAbsolutePath());
                    document.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;

            case "ToText":
                try {
                    PDDocument document = PDDocument.load(new File(saveDir + ".pdf"));
                    PDFTextStripper textStripper = new PDFTextStripper();
                    textStripper.setStartPage(1);
                    textStripper.setEndPage(1);

                    // Extract text from the first page
                    String text = textStripper.getText(document);
                    completedFile = new File(saveDir +".txt");
                    try (FileWriter writer = new FileWriter(completedFile)) {
                        writer.write(text);
                    }

                    System.out.println("Saved text of first page: " + completedFile.getAbsolutePath());
                    document.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;

            case "ToHTML":
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

                    completedFile = new File(saveDir  +".html");
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
                return "error:994";
        }
        //delete the pdf file
        File pdfFile = new File(saveDir + ".pdf");
        pdfFile.delete();
        String result = "none yet";
        // Upload the completed file to S3
        if (completedFile != null && completedFile.exists()) {
            try {
                result = id + "/" + completedFile.toPath();
                PutObjectRequest putRequest = PutObjectRequest.builder()
                        .bucket("resultbucket-aws1")
                        .key(id + "/" + completedFile.toPath())
                        .build();
                s3Client.putObject(putRequest, completedFile.toPath());
                System.out.println("Uploaded file to S3: " + completedFile.getName());
                completedFile.delete();
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
        return result;
    }
}