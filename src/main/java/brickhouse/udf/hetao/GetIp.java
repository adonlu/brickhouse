package brickhouse.udf.hetao;

/**
 * Copyright 2012 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/

import com.google.common.collect.Maps;
import net.ipip.ipdb.City;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


@Description(name = "get_address",
        value = "_FUNC_(ip_address, type) - Returns the address of ip. for type 0/empty:combine,1:city info,2:district info",
        extended = "return json")
public class GetIp extends GenericUDF {
    private static FSDataInputStream inCity=null;
    private static FSDataInputStream inDis=null;
    private static City db1=null;
    private static City db2=null;
    static{
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new Configuration());
            inCity = fs.open(new Path("/user/hive/udf/resources/chengshi.ipdb"));
            inDis = fs.open(new Path("/user/hive/udf/resources/quxian.ipdb"));
            db1 =new City(inCity);
            db2 =new City(inDis);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory();
        if (primitiveCategory != PrimitiveObjectInspector.PrimitiveCategory.STRING
                && primitiveCategory != PrimitiveObjectInspector.PrimitiveCategory.VOID) {
            throw new UDFArgumentTypeException(1,
                    "A string, char, varchar or null argument was expected");

        }
        if(arguments.length>1){
            PrimitiveObjectInspector.PrimitiveCategory primitiveCategory2 = ((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory();
            if (primitiveCategory2 != PrimitiveObjectInspector.PrimitiveCategory.INT
                    && primitiveCategory2 != PrimitiveObjectInspector.PrimitiveCategory.VOID) {
                throw new UDFArgumentTypeException(1,
                        "A int or null argument was expected");

            }
        }

        // return the inspector to check the return value of evaluate function
        return ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    @Override
    public Map<Text, Text> evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0].get() == null) {
            return null;
        }
        IntWritable type=null;
        if (arguments.length>1&&arguments[1].get() != null) {
            type=(IntWritable) arguments[1].get();
        }
        String s =  arguments[0].get().toString();
        Map<String ,String> tmpResult= Maps.newHashMap();
        try{
            tmpResult=db1.findMap(s ,"CN");
            //0:合并,1:城市库,2:区县库
            if(type!=null&&type.equals(new IntWritable(1))){

            }else if(type!=null&&type.equals(new IntWritable(2))){
                tmpResult=db2.findMap(s ,"CN");;
            }else{
                Map add=db2.findMap(s ,"CN");
                if(add!=null){
                    tmpResult.putAll(db2.findMap(s ,"CN"));
                }
            }
            Map<Text,Text> result=new HashMap<Text,Text>();
            if(tmpResult==null||tmpResult.isEmpty()){
                return result;
            }
            for(Map.Entry<String,String> entry:tmpResult.entrySet()){
                result.put(new Text(entry.getKey()),new Text(entry.getValue()));
            }
            return result;
        }catch(Exception e) {
            //do nothing
            return null;
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}