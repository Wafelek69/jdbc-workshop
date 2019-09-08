package jdbc;

import jdbc.exception.RepositoryException;
import jdbc.model.Student;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

class StudentsJDBCRepository {

  private final DataSource dataSource;

  StudentsJDBCRepository(DataSource datasource) {
    this.dataSource = datasource;
  }

  /**
   * Zaimplementuj `findAllStudents` tak by zwracała listę wszystkich studentów.
   */
  public List<Student> findAllStudents() {


    try (Connection connection = dataSource.getConnection();
         Statement statement = connection.createStatement();
         ResultSet rs = statement.executeQuery(
                 "SELECT id, first_name, last_name, birthdate FROM students")
    ){
      ArrayList<Student> students = new ArrayList<>();
      while (rs.next()){
        students.add(new Student(
                rs.getLong("id"),
                rs.getString("first_name"),
                rs.getString("last_name"),
                rs.getDate("birthdate").toLocalDate()
        ));
      }
      return students;

    }catch (SQLException e){
      //return new ArrayList<>();
      throw new RepositoryException(e);
    }



    //throw new UnsupportedOperationException("To be implemented.");
  }

  /**
   * Zaimplementuj `countStudents` tak by zwracała liczbę studentów.
   */
  public int countStudents() {
    try(
            Connection connection = dataSource.getConnection();
            Statement statement =connection.createStatement();
            ResultSet rs = statement.executeQuery(
                    "SELECT COUNT(*) AS cnt FROM students"
            )
    ) {
      if (rs.next()) {
        return rs.getInt("cnt");
      } else {
        throw new RepositoryException("Illegal state. Result is empty.");
      }
    } catch (SQLException e) {
      throw new RepositoryException(e);
    }
  }

  /**
   * Zaimplementuj `findStudentById` tak, by zwracało pusty `Optional` jeżeli student o danym **id** nie zostanie znaleziony.
   */
  public Optional<Student> findStudentById(long id) {
    try(
            Connection connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(
                    "SELECT id, first_name, last_name, birthdate FROM students " +
                            "WHERE id = ?"
            );
    ) {
      preparedStatement.setLong(1, id);
      try(ResultSet resultSet = preparedStatement.executeQuery()) {
        if(resultSet.next()) {
          return Optional.of(new Student(
                  resultSet.getLong("id"),
                  resultSet.getString("first_name"),
                  resultSet.getString("last_name"),
                  resultSet.getDate("birthdate").toLocalDate()
          ));
        }  else {
          return Optional.empty();
        }
      }

    } catch (SQLException e) {
      throw new RepositoryException(e);
    }
  }

  /**
   * Zaimplementuj metodę  `createStudent`, która będzie dodawać wiersz w bazie danych.
   * Pole `Id` przekazanego obiektu zawiera null. Metodę należy zaimplementować w ten sposób żeby uzyskać klucz główny z bazy.
   * Możesz do tego użyć metody `statement.getGeneratedKeys()`. Przekaż `Statement.RETURN_GENERATED_KEYS` jako trzeci parametr w `createStatement`.
   * Możesz wykonać akcje dodania wiersza i uzyskania klucza w obrębie jednej transakcji.
   */
  public Student createStudent(Student student) {
    throw new UnsupportedOperationException("To be implemented.");
  }

  /**
   * Zaimplementuj metodę `updateStudent`, która uaktulnia wiersz w bazie odpowiadający danemu obiektowi.
   * Sprawdź czy zapytanie rzeczywiście zmieniło jeden wiersz. Jeżeli nie to przerwij transakcję i rzuć wyjątek.
   */
  public Student updateStudent(Student student) {
      try (
              Connection connection = dataSource.getConnection();
              PreparedStatement updateStudentStmt
                      = connection.prepareStatement(
                      "UPDATE students SET first_name = ?," +
                              "last_name = ?," +
                              "birthdate = ?" +
                              " WHERE id = ?"
              );
      ) {
          connection.setAutoCommit(false);
          updateStudentStmt.setString(1, student.getFirstName());
          updateStudentStmt.setString(2, student.getLastName());
          updateStudentStmt.setDate(3, Date.valueOf(student.getBirthdate()));
          updateStudentStmt.setLong(4, student.getId());

          int changedCount = updateStudentStmt.executeUpdate();

          if (changedCount == 1) {
              connection.commit();
              return student;
          } else {
              connection.rollback();
              throw new RepositoryException("Couldn't find student with id=" + student.getId() + " to perform update.");
          }
      } catch (SQLException e) {
          throw new RepositoryException(e);
      }
  }

  /**
   * Zaimplementuj metodę `deleteStudent`, która usunie studenta uzywając id.
   * Jeżeli zostanie usuniętych więcej studentów niż 1, to wycofaj transakcję.
   */

      public void deleteStudent(long id) {
          try (
                  Connection connection = dataSource.getConnection();
                  PreparedStatement deleteStudentsClassesStmt
                          = connection.prepareStatement("DELETE FROM school_class_students " +
                          " WHERE student_id = ?");
                  PreparedStatement deleteStudentStmt
                          = connection.prepareStatement("DELETE FROM students WHERE id = ?");
          ) {
              connection.setAutoCommit(false);

              deleteStudentsClassesStmt.setLong(1, id);
              deleteStudentsClassesStmt.executeUpdate();

              deleteStudentStmt.setLong(1, id);
              int modified = deleteStudentStmt.executeUpdate();

              if(modified == 1) {
                  connection.commit();
              } else {
                  connection.rollback();
                  throw new RepositoryException("Couldn't find student with id=" + id + " to perform delete.");
              }
          } catch (SQLException e) {
              throw new RepositoryException(e);
          }
      }

  /**
   * Zaimplementuj metodę `findStudentsByName` pozwalającą wyszukać studentów po począku imienia lub nazwiska.
   */
  public List<Student> findStudentsByName(String name) {
    throw new UnsupportedOperationException("To be implemented.");
  }

  /**
   * Zaimplementuj metodę `findStudentsByTeacherId` pozwalającą znaleźć studentów po `id` uczącego ich nauczyciela.
   */
  public List<Student> findStudentsByTeacherId(long id) {
    try (
            Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(
                    "select students.id, students.first_name, students.last_name, students.birthdate from students " +
                            "join school_class_students on school_class_students.student_id = students.id " +
                            "join school_classes on school_classes.id = school_class_students.school_class_id " +
                            "where school_classes.teacher_id = ? "
            );
    ) {
      statement.setLong(1, id);
      try (ResultSet rs = statement.executeQuery()) {
        ArrayList<Student> students = new ArrayList<>();
        while (rs.next()) {
          students.add(new Student(
                  rs.getLong("id"),
                  rs.getString("first_name"),
                  rs.getString("last_name"),
                  rs.getDate("birthdate").toLocalDate()
          ));
        }
        return students;
      }
    } catch (SQLException e) {
      throw new RepositoryException(e);
    }
  }

  /**
   * Zaimplementuj `getAverageAge` zwracającą obliczony średni wiek studentów.
   */
  Optional<Double> getAverageAge(Date date) {
    throw new UnsupportedOperationException("To be implemented.");
  }

  /**
   * Stwórz procedurę, która anonimizuje dane wszystkich uczniów zastępująć nazwiska, wten sposób,
   * że pozostawia tylko pierwszą literę nazwiska oraz dodaje po niej kropkę. Metoda powinna przyjąć listę id wierszy,
   * które powinny zostać w ten sposób przetworzone. Wywołaj tą procedurę poprzez JDBC.
   */
  void anomize(long id) {

  }
}
