package com.qf.bigdata.realtime.util;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class CSVUtil implements Serializable {

    private final static Logger log = LoggerFactory.getLogger(CSVUtil.class);

    //public static final String AREA_CODE_CSV_FILE = "areacode/areacode.csv";

    public static final String AREA_CODE_GIS_LOCATION_CHINA_FILE = "areacode/china_gis_location.csv";

    public static final char QUOTE_TAB = '\t';

    public static final char QUOTE_COMMON = ',';

    public static final String NEW_LINE_SEPARATOR = "\n";

    /**
     * 读csv文件
     * @return
     * @throws Exception
     */
    public static List<Map<String,String>> readCSVFile(String path,char delimiter) throws Exception {
        List<Map<String,String>> results = new ArrayList<Map<String,String>>();
        Reader reader = null;
        try {
            //reader = Files.newBufferedReader(Paths.get(path));
            reader = new InputStreamReader(CSVUtil.class.getClassLoader().getResourceAsStream(path));

            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withDelimiter(delimiter)
                    .withIgnoreHeaderCase()
                    .withTrim());

            Map<String,Integer> header = csvParser.getHeaderMap();
            Set<String> colKeys = header.keySet();
            //System.out.println("header=["+header+"]");
            //System.out.println("keys=["+colKeys+"]");

            for (CSVRecord csvRecord : csvParser) {
                Map<String,String> values = csvRecord.toMap();
                for(String colKey : colKeys){
                    String colValue = csvRecord.get(colKey);
                    values.put(colKey,colValue);
                }
                //System.out.println("values=["+values+"]");
                results.add(values);
            }
        }catch(Exception e) {
            log.error("read.csvfile.err=${path}",path);
        }finally {
            if(null != reader){
                reader.close();
            }
        }

        return results;
    }

    /**
     * 写csv文件
     * @param path
     * @param headers
     * @param datas
     * @throws Exception
     */
    public static void writeCSVFile(String path,String[] headers,char delimiter, List<Object[]> datas,boolean append) throws Exception{
        BufferedWriter writer = null;
        CSVPrinter csvPrinter = null;
        try {
            writer = Files.newBufferedWriter(Paths.get(path));
            if(append){
                writer = Files.newBufferedWriter(Paths.get(path), StandardOpenOption.APPEND);
            }

            CSVFormat csvFormat = CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withHeader(headers)
                    .withRecordSeparator(NEW_LINE_SEPARATOR)
                    .withDelimiter(delimiter)
                    .withIgnoreHeaderCase()
                    .withTrim();

            csvPrinter = new CSVPrinter(writer, csvFormat);
            csvPrinter.printRecords(datas);
            csvPrinter.flush();

        }catch(Exception e) {
            log.error("read.csvfile.err={path}",path);
        }finally {
            if(null != csvPrinter){
                csvPrinter.close(true);
            }
            if(null != writer){
                writer.close();
            }
        }



    }

    public static void main(String[] args) throws Exception {

        String path = "";

        //List<Map<String,String>> areaCodes = readCSVFile(path,QUOTE_TAB);
        //System.out.println("area.size="+areaCodes.size());

        String[] headers = new String[]{};
        char delimiter = QUOTE_COMMON;
        List<Object[]> datas = null;
        boolean append = true;


    }

}
