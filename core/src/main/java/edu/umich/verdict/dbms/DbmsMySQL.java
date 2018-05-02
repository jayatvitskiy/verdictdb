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
import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.umich.verdict.relation.expr.ColNameExpr;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StackTraceReader;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DbmsMySQL extends DbmsJDBC 
{
	final int MAX_LENGTH=255;
	
	public DbmsMySQL(VerdictContext vc, String dbName, String host, String port, String schema, String user,
            String password, String jdbcClassName) throws VerdictException{
			super(vc, dbName, host, port, schema, user, password, jdbcClassName);
			}

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
   
	/*@Override
	public boolean tableNameIsCaseSensitive()
    {
    		return true;
    }*/
	
	
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
	    return String.format("mod(crc32(cast(%s%s%s as char(%d))),%d)", //1000 is an arbitrary max size. perhaps 2^16 is better
	        getQuoteString(), col, getQuoteString(),MAX_LENGTH, mod);
	}
   
	@Override
	public String modOfHash(List<String> columns, int mod) {
	    String concatStr = "";
	    for (int i = 0; i < columns.size(); ++i) {
	        String col = columns.get(i);
	        String castStr = String.format("cast(%s%s%s as char(%d)", 	//1000 is arbitrary
	            getQuoteString(), col, getQuoteString(),MAX_LENGTH);
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
	
	
	//Override because this uses CAST AS STRING in DbmsJDBC.java
	public long[] getGroupCount(TableUniqueName tableName, List<SortedSet<ColNameExpr>> columnSetList)
            throws VerdictException {
        if (columnSetList.isEmpty()) {
            return null;
        }
        int setCount = 1;
        long[] groupCounts = new long[columnSetList.size()];
        List<String> countStringList = new ArrayList<>();
        for (SortedSet<ColNameExpr> columnSet : columnSetList) {
            List<String> colStringList = new ArrayList<>();
            for (ColNameExpr col : columnSet) {
                String colString = String.format("COALESCE(CAST(%s as CHAR(%d), '%s')",
                        col.getCol(),MAX_LENGTH, NULL_STRING);
                colStringList.add(colString);
            }
            String concatString = "";
            for (int i = 0; i < colStringList.size(); ++i) {
                concatString += colStringList.get(i);
                if (i < colStringList.size() - 1) {
                    concatString += ", ";
                }
            }
            String countString = String.format("COUNT(DISTINCT(CONCAT_WS(',', %s))) as cnt%d",
                    concatString, setCount++);
            countStringList.add(countString);
        }
        String sql = "SELECT ";
        for (int i = 0; i < countStringList.size(); ++i) {
            sql += countStringList.get(i);
            if (i < countStringList.size() - 1) {
                sql += ", ";
            }
        }
        sql += String.format(" FROM %s", tableName);

        ResultSet rs;
        try {
            rs = executeJdbcQuery(sql);
            while (rs.next()) {
                for (int i = 0; i < groupCounts.length; ++i) {
                    groupCounts[i] = rs.getLong(i+1);
                }
            }
        } catch (SQLException e) {
            throw new VerdictException(StackTraceReader.stackTrace2String(e));
        }
        return groupCounts;
    }
	
	public void createMetaTablesInDBMS(TableUniqueName originalTableName, TableUniqueName sizeTableName,
            TableUniqueName nameTableName) throws VerdictException {
		VerdictLogger.debug(this, "Creates meta tables if not exist.");
		String sql = String.format("CREATE TABLE IF NOT EXISTS %s (schemaname TEXT, tablename TEXT, samplesize BIGINT, originaltablesize BIGINT)", sizeTableName); 
		executeUpdate(sql);
		
		sql = String.format("CREATE TABLE IF NOT EXISTS %s (originalschemaname TEXT, originaltablename TEXT, sampleschemaaname TEXT, sampletablename TEXT, sampletype TEXT, samplingratio DOUBLE, columnnames TEXT)", nameTableName); 
		executeUpdate(sql);	//can't use string, must use text
		
		VerdictLogger.debug(this, "Meta tables created.");
		vc.getMeta().refreshTables(sizeTableName.getDatabaseName());
		}
	
	@Override
    public void insertEntry(TableUniqueName tableName, List<Object> values) throws VerdictException {
        StringBuilder sql = new StringBuilder(1000);
        sql.append(String.format("insert into %s values ", tableName)); //slightly different syntax here
        sql.append("(");
        String with = "'";
        sql.append(Joiner.on(", ").join(StringManipulations.quoteString(values, with)));
        sql.append(")");
        executeUpdate(sql.toString());
    }
}
