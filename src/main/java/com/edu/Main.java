package com.edu;

import org.apache.pdfbox.pdmodel.PDDocument;

public class Main {
    public static void main(String[] args) {
        //print current dir
        System.out.println(System.getProperty("user.dir"));
        String action = "toHTML";
        String fileURL = "http://scheinerman.net/judaism/pesach/haggadah.pdf"; // Replace with your URL
        String saveDir = "midWork";

        try {
            PdfWorker.work(action ,fileURL, saveDir);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}