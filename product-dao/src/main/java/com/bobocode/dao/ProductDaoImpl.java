package com.bobocode.dao;

import com.bobocode.exception.DaoOperationException;
import com.bobocode.model.Product;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ProductDaoImpl implements ProductDao {
    private static final String INSERT_PRODUCT = "INSERT INTO products(name, producer, price, expiration_date) VALUES (?,?,?,?)";
    private static final String UPDATE_PRODUCT = "UPDATE products SET name=?, producer=?, price=?, expiration_date=? WHERE id=?";
    private static final String SELECT_PRODUCTS = "SELECT * FROM products";
    private static final String SELECT_PRODUCT = "SELECT * FROM products WHERE id=?";
    private static final String DELETE_PRODUCT = "DELETE FROM products WHERE id=?";
    private DataSource dataSource;

    public ProductDaoImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(Product product) {
        try (Connection connection = dataSource.getConnection()) {
            saveProduct(connection, product);
        } catch (SQLException e) {
            throw new DaoOperationException(String.format("Error saving product: %s", product, e));
        }
    }

    private void saveProduct(Connection connection, Product product) throws SQLException {
        PreparedStatement stm = prepareInsertStatement(connection, product);
        executeInsert(stm);
        Long id = getGeneratedId(stm);
        product.setId(id);
    }

    private PreparedStatement prepareInsertStatement(Connection connection, Product product) throws SQLException {
        PreparedStatement stm = connection.prepareStatement(INSERT_PRODUCT, PreparedStatement.RETURN_GENERATED_KEYS);
        fillPrepareStatement(product, stm);
        return stm;
    }

    private Long getGeneratedId(PreparedStatement stm) throws SQLException {
        ResultSet generatedKeys = stm.getGeneratedKeys();
        generatedKeys.next();
        return generatedKeys.getLong(1);
    }

    private void executeInsert(PreparedStatement stm) throws SQLException {
        int affectedRows = stm.executeUpdate();
        if (affectedRows == 0) {
            throw new DaoOperationException("Data was not saved!");
        }
    }

    private void fillPrepareStatement(Product product, PreparedStatement stm) throws SQLException {
        stm.setString(1, product.getName());
        stm.setString(2, product.getProducer());
        stm.setBigDecimal(3, product.getPrice());
        stm.setDate(4, Date.valueOf(product.getExpirationDate()));
    }

    @Override
    public List<Product> findAll() {
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement stm = connection.prepareStatement(SELECT_PRODUCTS);
            ResultSet resultSet = stm.executeQuery();
            return mapToProducts(resultSet);
        } catch (SQLException e) {
            throw new DaoOperationException(e.getMessage(), e);
        }
    }

    private List<Product> mapToProducts(ResultSet resultSet) throws SQLException {
        List<Product> products = new ArrayList<>();
        while (resultSet.next()) {
            products.add(mapToProduct(resultSet));
        }
        return products;
    }

    private Product mapToProduct(ResultSet rs) throws SQLException {
        return Product
                .builder()
                .id(rs.getLong(1))
                .name(rs.getString(2))
                .producer(rs.getString(3))
                .price(rs.getBigDecimal(4))
                .expirationDate(rs.getDate(5).toLocalDate())
                .creationTime(rs.getTimestamp(6).toLocalDateTime())
                .build();
    }

    @Override
    public Product findOne(Long id) {
        try (Connection connection = dataSource.getConnection()) {
            return getProductById(connection, id);
        } catch (SQLException e) {
            throw new DaoOperationException(e.getMessage(), e);
        }
    }

    private Product getProductById(Connection connection, Long id) throws SQLException {
        PreparedStatement stm = prepareSelectByIdStatement(connection, id);
        ResultSet resultSet = stm.executeQuery();
        if (resultSet.next()) {
            return mapToProduct(resultSet);
        } else {
            throw new DaoOperationException(String.format("Product with id = %s does not exist", id));
        }
    }

    private PreparedStatement prepareSelectByIdStatement(Connection connection, Long id) throws SQLException {
        PreparedStatement stm = connection.prepareStatement(SELECT_PRODUCT);
        stm.setLong(1, id);
        return stm;
    }

    @Override
    public void update(Product product) {
        if (product.getId() == null) {
            throw new DaoOperationException("Cannot find a product without ID");
        }
        try (Connection connection = dataSource.getConnection()) {
            updateProduct(connection, product);
        } catch (SQLException e) {
            throw new DaoOperationException(e.getMessage(), e);
        }
    }

    private void updateProduct(Connection connection, Product product) throws SQLException {
        PreparedStatement stm = prepareUpdateStatement(product, connection);
        int affectedRows = stm.executeUpdate();
        if (affectedRows == 0) {
            throw new DaoOperationException(String.format("Product with id = %s does not exist", product.getId()));
        }
    }

    private PreparedStatement prepareUpdateStatement(Product product, Connection connection) throws SQLException {
        PreparedStatement stm = connection.prepareStatement(UPDATE_PRODUCT);
        fillPrepareStatement(product, stm);
        stm.setLong(5, product.getId());
        return stm;
    }

    @Override
    public void remove(Product product) {
        if (product.getId() == null) {
            throw new DaoOperationException("Cannot find a product without ID");
        }
        try (Connection connection = dataSource.getConnection()) {
            deleteProduct(product, connection);
        } catch (SQLException e) {
            throw new DaoOperationException(e.getMessage(), e);
        }
    }

    private void deleteProduct(Product product, Connection connection) throws SQLException {
        PreparedStatement stm = connection.prepareStatement(DELETE_PRODUCT);
        stm.setLong(1, product.getId());
        int affectedRows = stm.executeUpdate();
        if (affectedRows == 0) {
            throw new DaoOperationException(String.format("Product with id = %d does not exist", product.getId()));
        }
    }

}
