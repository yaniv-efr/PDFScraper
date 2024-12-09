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

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.regions.Region;

public class PdfWorker {


        public static String work(String Action, String fileUrl, String id) {
        StringBuilder result = new StringBuilder();
        HttpURLConnection con = null;
        S3Client s3Client = null;
        String saveDir = UUID.randomUUID().toString();
        File pdfFile = new File(saveDir + ".pdf");
        File completedFile = null;

        try {
            // Check if the URL is valid
            HttpURLConnection.setFollowRedirects(false);
            con = (HttpURLConnection) new URL(fileUrl).openConnection();
            con.setRequestMethod("HEAD");
            con.setConnectTimeout(10000);
            con.setReadTimeout(10000);

            int responseCode = con.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                String errorDescription = getHttpErrorDescription(responseCode);
                String errorMessage = responseCode + "-" + errorDescription.replace(" ", "-");
                System.err.println("URL check failed: " + errorMessage);
                return errorMessage;
            }

            // Download the PDF
            try (InputStream inputStream = new URL(fileUrl).openStream();
                FileOutputStream fileOutputStream = new FileOutputStream(pdfFile)) {
                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    fileOutputStream.write(buffer, 0, bytesRead);
                }
            }
            result.append("Downloaded-PDF-file-to: ").append(pdfFile.getAbsolutePath().replace(" ", "-")).append("\n");

            // Perform the specified action
            try (PDDocument document = PDDocument.load(pdfFile)) {
                switch (Action) {
                    case "ToImage":
                        PDFRenderer pdfRenderer = new PDFRenderer(document);
                        BufferedImage bim = pdfRenderer.renderImageWithDPI(0, 300, ImageType.RGB);
                        completedFile = new File(saveDir + ".png");
                        ImageIO.write(bim, "png", completedFile);
                        result.append("Converted-first-page-to-image: ").append(completedFile.getAbsolutePath().replace(" ", "-")).append("\n");
                        break;

                    case "ToText":
                        PDFTextStripper textStripper = new PDFTextStripper();
                        textStripper.setStartPage(1);
                        textStripper.setEndPage(1);
                        String text = textStripper.getText(document);
                        completedFile = new File(saveDir + ".txt");
                        try (FileWriter writer = new FileWriter(completedFile)) {
                            writer.write(text);
                        }
                        result.append("Extracted-text-from-first-page: ").append(completedFile.getAbsolutePath().replace(" ", "-")).append("\n");
                        break;

                    case "ToHTML":
                        PDFTextStripper htmlStripper = new PDFTextStripper();
                        htmlStripper.setStartPage(1);
                        htmlStripper.setEndPage(1);
                        String htmlText = htmlStripper.getText(document)
                                .replace("&", "&amp;")
                                .replace("<", "&lt;")
                                .replace(">", "&gt;");
                        completedFile = new File(saveDir + ".html");
                        try (FileWriter writer = new FileWriter(completedFile)) {
                            writer.write("<html><body>" + htmlText + "</body></html>");
                        }
                        result.append("Converted-first-page-to-HTML: ").append(completedFile.getAbsolutePath().replace(" ", "-")).append("\n");
                        break;

                    default:
                        String unknownAction = "Unknown-action: " + Action.replace(" ", "-");
                        System.err.println(unknownAction);
                        return unknownAction;
                }
            }

            // Upload to S3 if a file was generated
            if (completedFile != null && completedFile.exists()) {
                s3Client = S3Client.builder().region(Region.US_EAST_1).build();
                String s3Key = id + "/" + completedFile.getName();
                PutObjectRequest putRequest = PutObjectRequest.builder()
                        .bucket("resultbucket-aws1")
                        .key(s3Key)
                        .build();
                s3Client.putObject(putRequest, completedFile.toPath());
                result.append("Uploaded-file-to-S3: ").append(s3Key.replace(" ", "-")).append("\n");
            } else {
                result.append("No-file-was-generated-to-upload.\n");
            }

        } catch (Exception e) {
            String errorMessage = "Error-encountered: " + e.getMessage().replace(" ", "-");
            System.err.println(errorMessage);
            e.printStackTrace();
            return errorMessage;
        } finally {
            // Cleanup resources
            if (con != null) {
                con.disconnect();
            }
            if (pdfFile.exists() && !pdfFile.delete()) {
                System.err.println("Failed-to-delete-PDF-file: " + pdfFile.getAbsolutePath().replace(" ", "-"));
            }
            if (completedFile != null && completedFile.exists() && !completedFile.delete()) {
                System.err.println("Failed-to-delete-generated-file: " + completedFile.getAbsolutePath().replace(" ", "-"));
            }
            if (s3Client != null) {
                s3Client.close();
            }
        }

        return result.toString();
    }

    private static String getHttpErrorDescription(int code) {
        switch (code) {
            case 400: return "Bad-Request";
            case 401: return "Unauthorized";
            case 403: return "Forbidden";
            case 404: return "Not-Found";
            case 500: return "Internal-Server-Error";
            case 502: return "Bad-Gateway";
            case 503: return "Service-Unavailable";
            case 504: return "Gateway-Timeout";
            default: return "HTTP-Error-" + code;
        }
    }


}
