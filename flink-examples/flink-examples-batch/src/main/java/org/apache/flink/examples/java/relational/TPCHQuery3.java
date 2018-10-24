/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.examples.java.relational;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * This program implements a modified version of the TPC-H query 3. The
 * example demonstrates how to assign names to fields by extending the Tuple class.
 * The original query can be found at
 * <a href="http://www.tpc.org/tpch/spec/tpch2.16.0.pdf">http://www.tpc.org/tpch/spec/tpch2.16.0.pdf</a> (page 29).
 *
 * <p>This program implements the following SQL equivalent:
 *
 * <p><pre>{@code
 * SELECT
 *      l_orderkey,
 *      SUM(l_extendedprice*(1-l_discount)) AS revenue,
 *      o_orderdate,
 *      o_shippriority
 * FROM customer,
 *      orders,
 *      lineitem
 * WHERE
 *      c_mktsegment = '[SEGMENT]'
 *      AND c_custkey = o_custkey
 *      AND l_orderkey = o_orderkey
 *      AND o_orderdate < date '[DATE]'
 *      AND l_shipdate > date '[DATE]'
 * GROUP BY
 *      l_orderkey,
 *      o_orderdate,
 *      o_shippriority;
 * }</pre>
 *
 * <p>Compared to the original TPC-H query this version does not sort the result by revenue
 * and orderdate.
 *
 * <p>Input files are plain text CSV files using the pipe character ('|') as field separator
 * as generated by the TPC-H data generator which is available at <a href="http://www.tpc.org/tpch/">http://www.tpc.org/tpch/</a>.
 *
 * <p>Usage: <code>TPCHQuery3 --lineitem&lt;path&gt; --customer &lt;path&gt; --orders&lt;path&gt; --output &lt;path&gt;</code><br>
 *
 * <p>This example shows how to use:
 * <ul>
 * <li> custom data type derived from tuple data types
 * <li> inline-defined functions
 * <li> build-in aggregation functions
 * </ul>
 */
@SuppressWarnings("serial")
public class TPCHQuery3 {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		if (!params.has("lineitem") && !params.has("customer") && !params.has("orders")) {
			System.err.println("  This program expects data from the TPC-H benchmark as input data.");
			System.err.println("  Due to legal restrictions, we can not ship generated data.");
			System.out.println("  You can find the TPC-H data generator at http://www.tpc.org/tpch/.");
			System.out.println("  Usage: TPCHQuery3 --lineitem <path> --customer <path> --orders <path> [--output <path>]");
			return;
		}

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().setGlobalJobParameters(params);

		// get input data
		DataSet<Lineitem> lineitems = getLineitemDataSet(env, params.get("lineitem"));
		DataSet<Customer> customers = getCustomerDataSet(env, params.get("customer"));
		DataSet<Order> orders = getOrdersDataSet(env, params.get("orders"));

		// Filter market segment "AUTOMOBILE"
		customers = customers.filter(
								new FilterFunction<Customer>() {
									@Override
									public boolean filter(Customer c) {
										return c.getMktsegment().equals("AUTOMOBILE");
									}
								});

		// Filter all Orders with o_orderdate < 12.03.1995
		orders = orders.filter(
							new FilterFunction<Order>() {
								private final DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
								private final Date date = format.parse("1995-03-12");

								@Override
								public boolean filter(Order o) throws ParseException {
									return format.parse(o.getOrderdate()).before(date);
								}
							});

		// Filter all Lineitems with l_shipdate > 12.03.1995
		lineitems = lineitems.filter(
								new FilterFunction<Lineitem>() {
									private final DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
									private final Date date = format.parse("1995-03-12");

									@Override
									public boolean filter(Lineitem l) throws ParseException {
										return format.parse(l.getShipdate()).after(date);
									}
								});

		// Join customers with orders and package them into a ShippingPriorityItem
		DataSet<ShippingPriorityItem> customerWithOrders =
				customers.join(orders).where(0).equalTo(1)
							.with(
								new JoinFunction<Customer, Order, ShippingPriorityItem>() {
									@Override
									public ShippingPriorityItem join(Customer c, Order o) {
										return new ShippingPriorityItem(o.getOrderKey(), 0.0, o.getOrderdate(),
												o.getShippriority());
									}
								});

		// Join the last join result with Lineitems
		DataSet<ShippingPriorityItem> result =
				customerWithOrders.join(lineitems).where(0).equalTo(0)
									.with(
											new JoinFunction<ShippingPriorityItem, Lineitem, ShippingPriorityItem>() {
												@Override
												public ShippingPriorityItem join(ShippingPriorityItem i, Lineitem l) {
													i.setRevenue(l.getExtendedprice() * (1 - l.getDiscount()));
													return i;
												}
											})
								// Group by l_orderkey, o_orderdate and o_shippriority and compute revenue sum
								.groupBy(0, 2, 3)
								.aggregate(Aggregations.SUM, 1);

		// emit result
		if (params.has("output")) {
			result.writeAsCsv(params.get("output"), "\n", "|");
			// execute program
			env.execute("TPCH Query 3 Example");
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			result.print();
		}

	}

	// *************************************************************************
	//     DATA TYPES
	// *************************************************************************

	/**
	 * Lineitem.
 	 */
	public static class Lineitem extends Tuple4<Long, Double, Double, String> {

		public Long getOrderkey() {
			return this.f0;
		}

		public Double getDiscount() {
			return this.f2;
		}

		public Double getExtendedprice() {
			return this.f1;
		}

		public String getShipdate() {
			return this.f3;
		}
	}

	/**
	 * Customer.
	 */
	public static class Customer extends Tuple2<Long, String> {

		public Long getCustKey() {
			return this.f0;
		}

		public String getMktsegment() {
			return this.f1;
		}
	}

	/**
	 * Order.
	 */
	public static class Order extends Tuple4<Long, Long, String, Long> {

		public Long getOrderKey() {
			return this.f0;
		}

		public Long getCustKey() {
			return this.f1;
		}

		public String getOrderdate() {
			return this.f2;
		}

		public Long getShippriority() {
			return this.f3;
		}
	}

	public static class ShippingPriorityItem extends Tuple4<Long, Double, String, Long> {

		public ShippingPriorityItem() {}

		public ShippingPriorityItem(Long orderkey, Double revenue,
				String orderdate, Long shippriority) {
			this.f0 = orderkey;
			this.f1 = revenue;
			this.f2 = orderdate;
			this.f3 = shippriority;
		}

		public Long getOrderkey() {
			return this.f0;
		}

		public void setOrderkey(Long orderkey) {
			this.f0 = orderkey;
		}

		public Double getRevenue() {
			return this.f1;
		}

		public void setRevenue(Double revenue) {
			this.f1 = revenue;
		}

		public String getOrderdate() {
			return this.f2;
		}

		public Long getShippriority() {
			return this.f3;
		}
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static DataSet<Lineitem> getLineitemDataSet(ExecutionEnvironment env, String lineitemPath) {
		return env.readCsvFile(lineitemPath)
					.fieldDelimiter("|")
					.includeFields("1000011000100000")
					.tupleType(Lineitem.class);
	}

	private static DataSet<Customer> getCustomerDataSet(ExecutionEnvironment env, String customerPath) {
		return env.readCsvFile(customerPath)
					.fieldDelimiter("|")
					.includeFields("10000010")
					.tupleType(Customer.class);
	}

	private static DataSet<Order> getOrdersDataSet(ExecutionEnvironment env, String ordersPath) {
		return env.readCsvFile(ordersPath)
					.fieldDelimiter("|")
					.includeFields("110010010")
					.tupleType(Order.class);
	}

}
