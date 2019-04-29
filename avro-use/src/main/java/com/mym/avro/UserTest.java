package com.mym.avro;

import com.bigdata.entity.product;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;

public class UserTest {

    public static void main(String[] args)throws Exception {
        //序列化
        product user=product.newBuilder().setCompanyName("zhangy").setDirection("blue").setProductId("1").build();
        product user2=product.newBuilder().setCompanyName("zhangy1").setDirection("blue1").setProductId("12").build();
        File file=new File("users.avro");
        DatumWriter<product> userDatumWriter = new SpecificDatumWriter<product>(product.class);
        DataFileWriter<product> dataFileWriter = new DataFileWriter<product>(userDatumWriter);
        dataFileWriter.create(user.getSchema(), new File("users.avro"));
        dataFileWriter.append(user);
        dataFileWriter.append(user2);
        dataFileWriter.close();
        //反序列化
        DatumReader<product> userDatumReader = new SpecificDatumReader<product>(product.class);
        DataFileReader<product> dataFileReader = new DataFileReader<product>(file, userDatumReader);
        product user1 = null;
        while (dataFileReader.hasNext()) {
            user1 = dataFileReader.next();
            System.out.println(user);
        }
    }
}
