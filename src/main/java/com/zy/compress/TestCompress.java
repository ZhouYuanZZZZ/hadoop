package com.zy.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.*;
import java.lang.reflect.InvocationTargetException;

public class TestCompress {

    private static void compress(String fileName,String compressType) throws IOException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

        FileInputStream fileInputStream = new FileInputStream(new File(fileName));

        Class<?> codecClass = Class.forName(compressType);
        CompressionCodec compressionCodec = (CompressionCodec) codecClass.getConstructor().newInstance();

        FileOutputStream fileOutputStream =
                new FileOutputStream(new File(fileName + compressionCodec.getDefaultExtension()));

        CompressionOutputStream compressionOutputStream = compressionCodec.createOutputStream(fileOutputStream);

        //压缩文件输入流
        IOUtils.copyBytes(fileInputStream,compressionOutputStream,1024*1024*10,false);

        IOUtils.closeStream(fileInputStream);
        IOUtils.closeStream(fileOutputStream);
        IOUtils.closeStream(compressionOutputStream);

    }

    private static void deCompress(String fileName) throws IOException {
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(fileName));

        if (codec == null) {
            System.out.println("cannot find codec for file " + fileName);
            return;
        }

        CompressionInputStream cis = codec.createInputStream(new FileInputStream(new File(fileName)));
        FileOutputStream fos = new FileOutputStream(new File("C:\\Users\\zy127\\Desktop\\hadoopFile\\compress\\doc.docx"));

        IOUtils.copyBytes(cis, fos, 1024*1024*5, false);

        IOUtils.closeStream(cis);
        IOUtils.closeStream(fos);
    }

    public static void main(String[] args) throws NoSuchMethodException, IOException, InstantiationException, IllegalAccessException, InvocationTargetException, ClassNotFoundException {

        //compress("C:\\Users\\zy127\\Desktop\\hadoopFile\\compress\\a.docx","org.apache.hadoop.io.compress.GzipCodec");
        deCompress("C:\\Users\\zy127\\Desktop\\hadoopFile\\compress\\a.docx.gz");
    }




}
