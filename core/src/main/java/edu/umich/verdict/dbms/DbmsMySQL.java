/*
 * Copyright 2017 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict.dbms;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Joiner;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StringManipulations;

public class DbmsMySQL extends DbmsJDBC 
{

	@Override
	protected String randomPartitionColumn() {
		//Differences from Hive:
		
		//MySQL's round() takes a second argument, the number of decimal places to round to, and thus returns
		//a DECIMAL. Since the point of our rounding is to convert a DECIMAL to an INTEGER, mySQL's floor() is
		//more appropriate.
		
		//DIV 1 is apparently a huge performance improvement over FLOOR, but it has some questionable behaviors
		//with negative values.
	    int pcount = partitionCount();
	    return String.format("mod(floor(rand(unix_timestamp())*%d), %d) AS %s",  
	            pcount, pcount, partitionColumnName());
	}
   
	@Override
	protected String randomNumberExpression(SampleParam param) {
		//This does not need to change for MySQL
	    String expr = "rand(unix_timestamp())";
	    return expr;
	}
   
	@Override
	public String modOfHash(String col, int mod) {
		//not sure if MySQL supports % for mod, so I replaced it with mod()
		//crc32 is supported in mySQL
		//cannot cast to varchar or text. can only cast to char, and must specify array size!
	    return String.format("mod(crc32(cast(%s%s%s as char(1000))),%d)", //1000 is an arbitrary max size. perhaps 2^16 is better
	        getQuoteString(), col, getQuoteString(), mod);
	}
   
	@Override
	public String modOfHash(List<String> columns, int mod) {
	    String concatStr = "";
	    for (int i = 0; i < columns.size(); ++i) {
	        String col = columns.get(i);
	        String castStr = String.format("cast(%s%s%s as char(1000)", 	//1000 is arbitrary
	            getQuoteString(), col, getQuoteString());
	        if (i < columns.size() - 1) {
	            castStr += ",";
	        }
	        concatStr += castStr;
	    }
	    return String.format("crc32(concat_ws('%s', %s)) %% %d", 
	        HASH_DELIM, concatStr, mod);
	}
	
	@Override
	protected String modOfRand(int mod) {
	    return String.format("mod(abs(rand(unix_timestamp())),d)", mod);
	}
}
