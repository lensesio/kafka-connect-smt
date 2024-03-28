package io.lenses.connect.smt.header;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class UtilsIsBlankTest {
  @Test
  public void isBlankReturnsTrueWhenStringIsNull() {
    assertTrue(Utils.isBlank(null));
  }

  @Test
  public void isBlankReturnsTrueWhenStringIsEmpty() {
    assertTrue(Utils.isBlank(""));
  }

  @Test
  public void isBlankReturnsFalseWhenStringIsNotBlank() {
    assertFalse(Utils.isBlank("not blank"));
  }
}
