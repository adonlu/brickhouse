package brickhouse.udf.date;

import brickhouse.udf.json.Json2MapUDF;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Map;

public class Json2MapUDFTest {

	@Test
	public void testJson2Map() {
		Json2MapUDF udf = new Json2MapUDF();
		String str="{\"param\":{\"homeworkFileUrl\":\"https:\",\"blockListUrl\":\"https://sbdata.hetao101.com/1173546-60-127.zip\"},\"context\":{\"currentUrl\":\"http://127.0.0.1:8888/index.html?locale=zh-cn\",\"stageDisplayMode\":\"small\",\"unitId\":0,\"lessonId\":0,\"courseId\":0},\"event_time\":1556448674945}";

		Map results1 = udf.evaluate(new String[]{str,"2"});
		Assert.assertEquals(10, results1.size());
		Map results2 = udf.evaluate(new String[]{str,"1"});
		System.out.println();
		Assert.assertEquals(3, results2.size());
	}

}
