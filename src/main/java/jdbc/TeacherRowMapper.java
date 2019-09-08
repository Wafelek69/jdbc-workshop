package jdbc;

import jdbc.model.Teacher;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TeacherRowMapper implements RowMapper<Teacher> {

  @Override
  public Teacher mapRow(ResultSet resultSet, int i) throws SQLException {
    throw new UnsupportedOperationException("To be implemented.");
  }
}
