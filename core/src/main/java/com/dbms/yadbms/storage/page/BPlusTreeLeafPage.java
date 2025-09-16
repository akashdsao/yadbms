package com.dbms.yadbms.storage.page;

import static com.dbms.yadbms.common.utils.Constants.INVALID_PAGE_ID;

import com.dbms.yadbms.common.exceptions.DBException;
import com.dbms.yadbms.common.exceptions.ErrorType;
import com.dbms.yadbms.config.PageId;
import java.util.Comparator;
import lombok.Getter;
import lombok.Setter;

public final class BPlusTreeLeafPage<K, V> extends BPlusTreePage {

  private final Object[] keys;
  private final RecordId[] rids; // RecordId pairs (pageId, slotNumber)
  private final Comparator<? super K> cmp;

  @Setter @Getter private PageId nextPageId = PageId.store(INVALID_PAGE_ID);

  /**
   * @param slotCount logical capacity (max key/RID pair)
   * @param comparator ordering for keys (avoid assuming K is Comparable)
   */
  public BPlusTreeLeafPage(int slotCount, Comparator<? super K> comparator) {
    this.keys = new Object[slotCount];
    this.rids = new RecordId[slotCount];
    this.cmp = comparator;
    init(slotCount);
  }

  public void init(int maxSize) {
    setPageType(IndexPageType.LEAF_PAGE);
    setSize(0);
    setMaxSize(Math.min(maxSize, keys.length));
    this.nextPageId = PageId.store(INVALID_PAGE_ID);
  }

  /** key indices valid: [0 .. size-1] */
  @SuppressWarnings("unchecked")
  public K keyAt(int index) {
    if (index < 0 || index >= getSize()) {
      throw new DBException(
          ErrorType.INDEX_NOT_FOUND,
          "Invalid key index: " + index + " (valid: 0.." + (getSize() - 1) + ")");
    }
    return (K) keys[index];
  }

  /** rid indices valid: [0 .. size-1] */
  public RecordId ridAt(int index) {
    if (index < 0 || index >= getSize()) {
      throw new DBException(
          ErrorType.INDEX_NOT_FOUND,
          "Invalid rid index: " + index + " (valid: 0.." + (getSize() - 1) + ")");
    }
    return rids[index];
  }

  /** Writes key at index (within capacity, not necessarily within current size). */
  public void setKeyAt(int index, K key) {
    if (index < 0 || index >= getMaxSize()) {
      throw new DBException(
          ErrorType.INDEX_NOT_FOUND,
          "Invalid key index for write: " + index + " (valid: 0.." + (getMaxSize() - 1) + ")");
    }
    keys[index] = key;
  }

  /** Writes RID at index (within capacity, not necessarily within current size). */
  public void setRidAt(int index, RecordId rid) {
    if (index < 0 || index >= getMaxSize()) {
      throw new DBException(
          ErrorType.INDEX_NOT_FOUND,
          "Invalid rid index for write: " + index + " (valid: 0.." + (getMaxSize() - 1) + ")");
    }
    rids[index] = rid;
  }

  /** Insert (key,rid) at an exact index, shifting tail to the right. */
  public void insertAt(int index, K key, RecordId rid) {
    final int sz = getSize();
    if (sz >= getMaxSize()) throw new IllegalStateException("Leaf page full");
    if (index < 0 || index > sz)
      throw new IndexOutOfBoundsException("insert index out of range: " + index);

    if (sz - index > 0) {
      System.arraycopy(keys, index, keys, index + 1, sz - index);
      System.arraycopy(rids, index, rids, index + 1, sz - index);
    }
    keys[index] = key;
    rids[index] = rid;
    setSize(sz + 1);
  }

  /** Insert (key,rid) in sorted order; returns the insertion index. */
  public int insertSorted(K key, RecordId rid) {
    int idx = lowerBound(key); // first i where key <= keyArray[i]
    insertAt(idx, key, rid);
    return idx;
  }

  /** Remove entry at index, shifting left. */
  public void removeAt(int index) {
    final int sz = getSize();
    if (index < 0 || index >= sz)
      throw new DBException(ErrorType.INDEX_NOT_FOUND, "remove index out of range: " + index);

    if (sz - 1 - index > 0) {
      System.arraycopy(keys, index + 1, keys, index, sz - 1 - index);
      System.arraycopy(rids, index + 1, rids, index, sz - 1 - index);
    }
    keys[sz - 1] = null;
    rids[sz - 1] = null;
    setSize(sz - 1);
  }

  /** Binary search: return index of key if present, else -1. */
  public int findKey(K key) {
    int lo = 0, hi = getSize(); // hi exclusive
    while (lo < hi) {
      int mid = (lo + hi) >>> 1;
      @SuppressWarnings("unchecked")
      K midKey = (K) keys[mid];
      int c = cmp.compare(key, midKey);
      if (c <= 0) hi = mid;
      else lo = mid + 1;
    }
    if (lo < getSize()) {
      @SuppressWarnings("unchecked")
      K at = (K) keys[lo];
      if (cmp.compare(key, at) == 0) return lo;
    }
    return -1;
  }

  /** First index i where key <= keyArray[i]. If all existing keys are less, returns size. */
  private int lowerBound(K key) {
    int lo = 0, hi = getSize();
    while (lo < hi) {
      int mid = (lo + hi) >>> 1;
      @SuppressWarnings("unchecked")
      K midKey = (K) keys[mid];
      if (cmp.compare(key, midKey) <= 0) hi = mid;
      else lo = mid + 1;
    }
    return lo;
  }

  /** Get record at a given rid index. */
  public V getRecord(int index) {
    @SuppressWarnings("unchecked")
    V value = (V) ridAt(index);
    return value;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("(");
    boolean first = true;
    for (int i = 0; i < getSize(); i++) {
      if (!first) sb.append(',');
      sb.append(keys[i]).append("->").append(rids[i]);
      first = false;
    }
    sb.append(')');
    return sb.toString();
  }
}
