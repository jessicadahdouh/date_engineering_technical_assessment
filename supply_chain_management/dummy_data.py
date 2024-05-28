from sqlalchemy import create_engine, Column, Integer, String, Numeric, Date, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker
from faker import Faker
from random import randint
from datetime import datetime

# Create the SQLAlchemy engine
engine = create_engine('postgresql://user:pass@localhost:5432/supply_chain')


Base = declarative_base()


class Supplier(Base):
    __tablename__ = 'suppliers'
    __table_args__ = {'schema': 'supply_chain_schema'}

    supplier_id = Column(Integer, primary_key=True)
    supplier_name = Column(String)


class Product(Base):
    __tablename__ = 'products'
    __table_args__ = {'schema': 'supply_chain_schema'}

    product_id = Column(Integer, primary_key=True)
    product_name = Column(String)
    category = Column(String)
    unit_price = Column(Numeric)


class Warehouse(Base):
    __tablename__ = 'warehouses'
    __table_args__ = {'schema': 'supply_chain_schema'}

    warehouse_id = Column(Integer, primary_key=True)
    warehouse_name = Column(String)


class Order(Base):
    __tablename__ = 'orders'
    __table_args__ = {'schema': 'supply_chain_schema'}

    order_id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('supply_chain_schema.products.product_id'))
    supplier_id = Column(Integer, ForeignKey('supply_chain_schema.suppliers.supplier_id'))
    quantity = Column(Integer)
    order_date = Column(Date)


class Shipment(Base):
    __tablename__ = 'shipments'
    __table_args__ = {'schema': 'supply_chain_schema'}

    shipment_id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey('supply_chain_schema.orders.order_id'))
    warehouse_id = Column(Integer, ForeignKey('supply_chain_schema.warehouses.warehouse_id'))
    shipment_date = Column(Date)


class Time(Base):
    __tablename__ = 'time'
    __table_args__ = {'schema': 'supply_chain_schema'}

    date_id = Column(Integer, primary_key=True)
    shipment_date = Column(Date)
    shipping_time = Column(Integer)


# Create the tables in the database
Base.metadata.create_all(engine)


Session = sessionmaker(bind=engine)
session = Session()

# Create a Faker instance to generate dummy data
fake = Faker()


def generate_dummy_data():
    # Generate dummy data for Suppliers table
    for _ in range(5):
        supplier = Supplier(supplier_name=fake.company())
        session.add(supplier)

    # Generate dummy data for Products table
    products = []
    for _ in range(5):
        product = Product(
            product_name=fake.word(),
            category=fake.word(),
            unit_price=randint(10, 100)
        )
        products.append(product)
        session.add(product)
    session.flush()

    # Generate dummy data for Warehouses table
    for _ in range(5):
        warehouse = Warehouse(warehouse_name=fake.city() + ' Warehouse')
        session.add(warehouse)

    # Generate dummy data for Orders table
    for _ in range(10):
        product = products[randint(0, len(products) - 1)]  # Select a random product
        order = Order(
            product_id=product.product_id,
            supplier_id=randint(1, 3),
            quantity=randint(10, 100),
            order_date=fake.date_between(start_date='-30d', end_date='today')
        )
        session.add(order)

    # Generate dummy data for Shipments table
    for order in session.query(Order).all():
        shipment = Shipment(
            order_id=order.order_id,
            warehouse_id=randint(1, 3),
            shipment_date=fake.date_between_dates(order.order_date, datetime.now())
        )
        session.add(shipment)

    # Generate dummy data for Time table
    for shipment in session.query(Shipment).all():
        shipping_time = randint(1, 7)  # Random shipping time between 1 to 7 days
        shipment_date = shipment.shipment_date
        time = Time(
            shipment_date=shipment_date,
            shipping_time=shipping_time
        )
        session.add(time)

    # Commit the changes to the database
    session.commit()


generate_dummy_data()


session.close()
