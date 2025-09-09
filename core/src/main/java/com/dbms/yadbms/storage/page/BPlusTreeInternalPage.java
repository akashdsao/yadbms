package com.dbms.yadbms.storage.page;

import com.dbms.yadbms.common.exceptions.DBException;
import com.dbms.yadbms.common.exceptions.ErrorType;
import com.dbms.yadbms.config.PageId;
import java.util.Comparator;

public final class BPlusTreeInternalPage<K> extends BPlusTreePage {

  private final Object[] keys;
  private final PageId[] childPageIds;
  private final Comparator<? super K> cmp;

  public BPlusTreeInternalPage(int slotCount, Comparator<? super K> comparator) {
    this.keys = new Object[slotCount];
    this.childPageIds = new PageId[slotCount];
    this.cmp = comparator;
    init(slotCount); // set pageType, size, maxSize
  }

  public void init(int maxSize) {
    setPageType(IndexPageType.INTERNAL_PAGE);
    setSize(0);
    setMaxSize(Math.min(maxSize, keys.length));
  }

  /** Valid read range: 1 .. getSize()-1 (key[0] is invalid/sentinel). */
  @SuppressWarnings("unchecked")
  public K keyAt(int index) {
    if (index < 0 || index >= getSize()) {
      throw new DBException(
          ErrorType.INDEX_NOT_FOUND,
          "Invalid key index: " + index + " (valid: 0.." + (getSize() - 1) + ")");
    }
    return (K) keys[index];
  }

  /** Valid write range: 1 .. getMaxSize()-1. */
  public void setKeyAt(int index, K key) {
    if (index < 0 || index >= getMaxSize()) {
      throw new DBException(
          ErrorType.INDEX_NOT_FOUND,
          "Invalid key index for write: " + index + " (valid: 0.." + (getMaxSize() - 1) + ")");
    }
    keys[index] = key;
  }

  /** Children valid range: 0 .. getSize()-1. */
  public PageId valueAt(int index) {
    if (index < 0 || index >= getSize()) {
      throw new DBException(
          ErrorType.INDEX_NOT_FOUND,
          "Invalid key child index: " + index + " (valid: 0.." + (getSize() - 1) + ")");
    }
    return childPageIds[index];
  }

  /** Children write range: 0 .. getMaxSize()-1. */
  public void setValueAt(int index, int pageId) {
    if (index < 0 || index >= getMaxSize()) {
      throw new DBException(
          ErrorType.INDEX_NOT_FOUND,
          "Invalid key child index for write: "
              + index
              + " (valid: 0.."
              + (getMaxSize() - 1)
              + ")");
    }
    childPageIds[index] = PageId.store(pageId);
  }

  /** Left-most child setter; if first child, bump size to 1. */
  public void setLeftmostChild(int pageId) {
    childPageIds[0] = PageId.store(pageId);
    if (getSize() == 0) setSize(1);
  }

  /**
   * Choose the child pointer for 'key' using BusTub's internal node layout. keys valid at
   * [1..size-1]; children at [0..size-1]. Returns child index 'idx' where idx = max{ j | key >=
   * keyAt(j) } with j in [1..size-1], default 0.
   */
  public PageId getChildForKey(K key) {
    final int size = getSize(); // number of children
    if (size <= 0) {
      throw new DBException(ErrorType.INDEX_NOT_FOUND, "Corrupt internal page: size <= 0");
    }

    int lo = 1, hi = size - 1; // search over valid keys
    int idx = 0; // default to leftmost child
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      K midKey = keyAt(mid);
      int c = cmp.compare(key, midKey);
      if (c < 0) {
        hi = mid - 1;
      } else {
        idx = mid; // key >= keyAt(mid) → go right; remember candidate
        lo = mid + 1;
      }
    }
    return valueAt(idx);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("(");
    boolean first = true;
    for (int i = 1; i < getSize(); i++) {
      if (!first) sb.append(',');
      sb.append(keys[i]);
      first = false;
    }
    sb.append(')');
    return sb.toString();
  }
}
