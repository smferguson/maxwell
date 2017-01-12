package com.zendesk.maxwell.schema;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.List;

import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.MaxwellTestWithIsolatedServer;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;

import com.google.code.or.common.util.MySQLConstants;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.*;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;

public class SchemaCaptureTest extends MaxwellTestWithIsolatedServer {
	private SchemaCapturer capturer;

	@Before
	public void setUp() throws Exception {
		server.getConnection().createStatement().executeUpdate("CREATE DATABASE if not exists test");
		this.capturer = new SchemaCapturer(server.getConnection(), CaseSensitivity.CASE_SENSITIVE);
	}

	@Test
	public void testDatabases() throws SQLException, InvalidSchemaError {
		Schema s = capturer.capture();
		String dbs = StringUtils.join(s.getDatabaseNames().iterator(), ":");

		if ( server.getVersion().equals("5.7") )
			assertEquals("maxwell:mysql:shard_1:shard_2:sys:test", dbs);
		else
			assertEquals("maxwell:mysql:shard_1:shard_2:test", dbs);
	}

	@Test
	public void testOneDatabase() throws SQLException, InvalidSchemaError {
		SchemaCapturer sc = new SchemaCapturer(server.getConnection(), CaseSensitivity.CASE_SENSITIVE, "shard_1");
		Schema s = sc.capture();

		String dbs = StringUtils.join(s.getDatabaseNames().iterator(), ":");
		assertEquals("shard_1", dbs);
	}

	@Test
	public void testTables() throws SQLException, InvalidSchemaError {
		Schema s = capturer.capture();

		Database shard1DB = s.findDatabase("shard_1");
		assert(shard1DB != null);

		List<String> nameList = shard1DB.getTableNames();

		assertEquals("ints:mediumints:minimal:sharded", StringUtils.join(nameList.iterator(), ":"));
	}

	@Test
	public void testColumns() throws SQLException, InvalidSchemaError {
		Schema s = capturer.capture();

		Table sharded = s.findDatabase("shard_1").findTable("sharded");
		assert(sharded != null);

		ColumnDef columns[];

		columns = sharded.getColumnList().toArray(new ColumnDef[0]);

		assertThat(columns[0], notNullValue());
		assertThat(columns[0], instanceOf(BigIntColumnDef.class));
		assertThat(columns[0].getName(), is("id"));
		assertEquals(0, columns[0].getPos());

		assertTrue(columns[0].matchesMysqlType(MySQLConstants.TYPE_LONGLONG));
		assertFalse(columns[0].matchesMysqlType(MySQLConstants.TYPE_DECIMAL));

		assertThat(columns[1], allOf(notNullValue(), instanceOf(IntColumnDef.class)));
		assertThat(columns[1].getName(), is("account_id"));
		assertThat(columns[1], instanceOf(IntColumnDef.class));
		assertThat(((IntColumnDef) columns[1]).isSigned(), is(false));

		if ( server.getVersion().equals("5.6") ) {
			assertThat(columns[10].getName(), is("timestamp2_field"));
			assertThat(columns[10], instanceOf(DateTimeColumnDef.class));
			assertThat(((DateTimeColumnDef) columns[10]).getColumnLength(), is(3L));

			assertThat(columns[11].getName(), is("datetime2_field"));
			assertThat(columns[11], instanceOf(DateTimeColumnDef.class));
			assertThat(((DateTimeColumnDef) columns[11]).getColumnLength(), is(6L));

			assertThat(columns[12].getName(), is("time2_field"));
			assertThat(columns[12], instanceOf(TimeColumnDef.class));
			assertThat(((TimeColumnDef) columns[12]).getColumnLength(), is(6L));
		}
	}

	@Test
	public void testPKs() throws SQLException, InvalidSchemaError {
		Schema s = capturer.capture();

		Table sharded = s.findDatabase("shard_1").findTable("sharded");
		List<String> pk = sharded.getPKList();
		assertThat(pk, notNullValue());
		assertThat(pk.size(), is(2));
		assertThat(pk.get(0), is("id"));
		assertThat(pk.get(1), is("account_id"));
	}
}
