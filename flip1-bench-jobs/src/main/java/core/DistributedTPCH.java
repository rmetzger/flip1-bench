package core;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.SplittableIterator;

import io.prestosql.tpch.Customer;
import io.prestosql.tpch.CustomerGenerator;
import io.prestosql.tpch.LineItem;
import io.prestosql.tpch.LineItemGenerator;
import io.prestosql.tpch.Nation;
import io.prestosql.tpch.NationGenerator;
import io.prestosql.tpch.Order;
import io.prestosql.tpch.OrderGenerator;
import io.prestosql.tpch.Part;
import io.prestosql.tpch.PartGenerator;
import io.prestosql.tpch.PartSupplier;
import io.prestosql.tpch.PartSupplierGenerator;
import io.prestosql.tpch.Region;
import io.prestosql.tpch.RegionGenerator;
import io.prestosql.tpch.Supplier;
import io.prestosql.tpch.SupplierGenerator;


public class DistributedTPCH {
	private double scale;
	private ExecutionEnvironment env;


	public DistributedTPCH(ExecutionEnvironment env) {
		this.env = env;
	}

	public void setScale(double scale) {
		this.scale = scale;
	}

	public double getScale() {
		return scale;
	}

	public DataSet<Part> generateParts() {
		return getGenerator(PartGenerator.class, Part.class);
	}

	public DataSet<LineItem> generateLineItems() {
		return getGenerator(LineItemGenerator.class, LineItem.class);
	}
	public DataSet<Order> generateOrders() {
		return getGenerator(OrderGenerator.class, Order.class);
	}

	public DataSet<Supplier> generateSuppliers() {
		return getGenerator(SupplierGenerator.class, Supplier.class);
	}

	public DataSet<Region> generateRegions() {
		return getGenerator(RegionGenerator.class, Region.class);
	}

	public DataSet<Nation> generateNations() {
		return getGenerator(NationGenerator.class, Nation.class);
	}

	public DataSet<Customer> generateCustomers() {
		return getGenerator(CustomerGenerator.class, Customer.class);
	}

	public DataSet<PartSupplier> generatePartSuppliers() {
		return getGenerator(PartSupplierGenerator.class, PartSupplier.class);
	}

	public <T> DataSet<T> getGenerator(Class<? extends Iterable<T>> generatorClass, Class<T> type) {
		SplittableIterator<T> si = new TPCHGeneratorSplittableIterator(scale, env.getParallelism(), generatorClass);
		return env.fromParallelCollection(si, type).name("Generator: "+generatorClass);
	}
}
