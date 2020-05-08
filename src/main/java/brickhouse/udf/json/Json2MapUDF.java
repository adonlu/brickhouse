package brickhouse.udf.json;
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
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Given a JSON String containing a map with values of all the same type,
 * return a Hive map of key-value pairs
 */

@Description(name = "json_to_map",
        value = "_FUNC_(json) - Returns a map of key-value pairs from a JSON string"
)
public class Json2MapUDF<main> extends UDF {

    public Map<String, String> evaluate(String[] arguments)  {
        int size=0;
        if(arguments.length==2){
            size=Integer.parseInt(arguments[1]);

        }
        return convert(arguments[0],size);

    }


    private static Map<String, String> convert(String str,int size) {
        int itetimes=0;
        Map<String, String> map = new HashMap<String, String>();
        try{
            JSONObject jsondata = JSONObject.fromObject(str);
            convertJson(jsondata, map,"",itetimes,size);
        }catch(Exception e){
            return Maps.newHashMap();
        }
        return map;
    }

    private static void convertJson(JSONObject json, Map<String, String> map, String prefix,int itetimes,int limit) {
        itetimes++;
        if(limit>0&&itetimes>limit){
            return;
        }
        for (Iterator<?> iter = json.keys(); iter.hasNext();) {
            String key = (String) iter.next();
            String value = json.getString(key);
            map.put(prefix+"_"+key, value);
                try {
                    JSONObject.fromObject(value);
                    convertJson(JSONObject.fromObject(value), map,prefix+"_"+key,itetimes,limit);
                } catch (JSONException e) {
                   /*last key*/
                }


        }
    }

}
