package com.dbms.yadbms.storage.index;

import static com.dbms.yadbms.common.utils.Constants.INVALID_PAGE_ID;

import com.dbms.yadbms.buffer.BufferPoolManager;
import com.dbms.yadbms.common.exceptions.DBException;
import com.dbms.yadbms.common.exceptions.ErrorType;
import com.dbms.yadbms.config.PageId;
import com.dbms.yadbms.storage.page.BPlusTreeHeaderPage;
import com.dbms.yadbms.storage.page.BPlusTreeInternalPage;
import com.dbms.yadbms.storage.page.BPlusTreeLeafPage;
import com.dbms.yadbms.storage.page.BPlusTreePage;
import com.dbms.yadbms.storage.page.ReadPageGuard;
import com.dbms.yadbms.storage.page.WritePageGuard;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BPlusTree<K> {
  @Getter private final String name;

  private final PageId headerPageId;
  private final BufferPoolManager bufferPoolManager;
  private final Comparator<? super K> keyComparator;
  private final int leafMaxSize;
  private final int internalMaxSize;

  public BPlusTree(
      String name,
      PageId headerPageId,
      BufferPoolManager bufferPoolManager,
      Comparator<? super K> keyComparator,
      int leafMaxSize,
      int internalMaxSize) {
    this.name = name;
    this.headerPageId = headerPageId;
    this.bufferPoolManager = bufferPoolManager;
    this.keyComparator = keyComparator;
    this.leafMaxSize = leafMaxSize;
    this.internalMaxSize = internalMaxSize;

    // Initialize header root pointer as INVALID on first open/creation
    try (WritePageGuard g = mustWrite(headerPageId)) {
      BPlusTreeHeaderPage header = g.asMut(BPlusTreeHeaderPage.class);
      if (header.getRootPageId() == null) {
        header.setRootPageId(PageId.store(INVALID_PAGE_ID));
      }
    } catch (Exception e) {
      throw new DBException(ErrorType.IO_ERROR, "unable to write to index header page", e);
    }
  }

  public PageId getRootPageId() {
    try (ReadPageGuard g = mustRead(headerPageId)) {
      BPlusTreeHeaderPage h = g.getDataAs(BPlusTreeHeaderPage.class);
      return h.getRootPageId();
    } catch (Exception e) {
      throw new DBException(ErrorType.IO_ERROR, "Unable to read index header page", e);
    }
  }

  public boolean isEmpty() {
    PageId r = getRootPageId();
    return r == null || r.getValue() == INVALID_PAGE_ID;
  }

  /** Point-lookup: returns true if found and writes into output[0]. */
  public <V> boolean getValue(K key, List<V> output) {
    output.clear();
    if (isEmpty()) return false;

    final PageId root = getRootPageId();
    if (root == null || root.getValue() == INVALID_PAGE_ID) return false;

    PageId pid = root;
    while (true) {
      try (ReadPageGuard guard = mustRead(pid)) {
        BPlusTreePage pageHdr = guard.getDataAs(BPlusTreePage.class);

        if (pageHdr.isLeafPage()) {
          @SuppressWarnings("unchecked")
          BPlusTreeLeafPage<K, V> leaf = guard.getDataAs(BPlusTreeLeafPage.class);
          int idx = leaf.findKey(key);
          if (idx < 0) return false;
          output.add(leaf.getRecord(idx));
          return true;
        } else {
          @SuppressWarnings("unchecked")
          BPlusTreeInternalPage<K> internal = guard.getDataAs(BPlusTreeInternalPage.class);
          pid = internal.getChildForKey(key); // choose next child by binary search
        }
      } catch (Exception e) {
        throw new DBException(ErrorType.IO_ERROR, "getValue failed: " + e.getMessage(), e);
      }
    }
  }

  /**
   * Insert unique (key, value). Returns false if duplicate key exists. NOTE: This implementation
   * assumes V == RecordId (as per your leaf API).
   */
  public <V> boolean insert(K key, V value) {
    if (isEmpty()) {
      startNewTree(key, value);
      return true;
    }

    // Descend to target leaf
    PageId pid = getRootPageId();
    while (true) {
      try (ReadPageGuard rg = mustRead(pid)) {
        BPlusTreePage hdr = rg.getDataAs(BPlusTreePage.class);
        if (hdr.isLeafPage()) break;
        @SuppressWarnings("unchecked")
        BPlusTreeInternalPage<K> internal = rg.getDataAs(BPlusTreeInternalPage.class);
        pid = internal.getChildForKey(key);
      } catch (Exception e) {
        throw new DBException(ErrorType.IO_ERROR, "insert descent failed: " + e.getMessage(), e);
      }
    }

    // Insert into leaf
    try (WritePageGuard lg = mustWrite(pid)) {
      @SuppressWarnings("unchecked")
      BPlusTreeLeafPage<K, V> leaf = lg.asMut(BPlusTreeLeafPage.class);

      // Reject duplicate
      int pos = leaf.findKey(key);
      if (pos >= 0) return false;

      // Insert sorted; may overflow
      leaf.insertSorted(key, castRecord(value));

      if (leaf.getSize() <= leaf.getMaxSize()) {
        return true;
      }

      // Split leaf
      SplitLeafResult<K> split = splitLeaf(pid, leaf);
      // Insert separator into parent (propagate if needed)
      insertIntoParent(pid, split.pushUpKey, split.newRightPid);
      return true;

    } catch (Exception e) {
      throw new DBException(ErrorType.IO_ERROR, "insert failed: " + e.getMessage(), e);
    }
  }

  /** Remove a key (unique). Silent if key absent. */
  public void remove(K key) {
    if (isEmpty()) return;

    // Descend to leaf
    PageId pid = getRootPageId();
    while (true) {
      try (ReadPageGuard rg = mustRead(pid)) {
        BPlusTreePage hdr = rg.getDataAs(BPlusTreePage.class);
        if (hdr.isLeafPage()) break;
        @SuppressWarnings("unchecked")
        BPlusTreeInternalPage<K> internal = rg.getDataAs(BPlusTreeInternalPage.class);
        pid = internal.getChildForKey(key);
      } catch (Exception e) {
        throw new DBException(ErrorType.IO_ERROR, "remove descent failed: " + e.getMessage(), e);
      }
    }

    // Delete in leaf
    try (WritePageGuard lg = mustWrite(pid)) {
      @SuppressWarnings("unchecked")
      BPlusTreeLeafPage<K, Object> leaf = lg.asMut(BPlusTreeLeafPage.class);
      int idx = leaf.findKey(key);
      if (idx < 0) return; // not present

      leaf.removeAt(idx);

      // Root-only leaf special case → if empty, clear header
      if (pid.equals(getRootPageId())) {
        if (leaf.getSize() == 0) {
          try (WritePageGuard hg = mustWrite(headerPageId)) {
            BPlusTreeHeaderPage h = hg.asMut(BPlusTreeHeaderPage.class);
            h.setRootPageId(PageId.store(INVALID_PAGE_ID));
          }
        }
        return;
      }

      // Underflow handling (basic): try to keep it simple for now.
      // Full BusTub behavior would: borrow from sibling, else merge and update parent.
      if (leaf.getSize() < leaf.getMinSize()) {
        handleLeafUnderflow(pid);
      }
    } catch (Exception e) {
      throw new DBException(ErrorType.IO_ERROR, "remove failed: " + e.getMessage(), e);
    }
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /** Bootstrap a brand-new tree: create a root leaf and publish header rootPageId. */
  private <V> void startNewTree(K key, V value) {
    try (WritePageGuard lg = mustNewLeafPage()) {
      PageId newRootPid = lg.getPageId(); // <-- adapt if your guard exposes page id differently
      @SuppressWarnings("unchecked")
      BPlusTreeLeafPage<K, V> root = lg.asMut(BPlusTreeLeafPage.class);
      root.init(leafMaxSize);
      root.insertSorted(key, castRecord(value));

      try (WritePageGuard hg = mustWrite(headerPageId)) {
        BPlusTreeHeaderPage header = hg.asMut(BPlusTreeHeaderPage.class);
        header.setRootPageId(newRootPid);
      }
    } catch (Exception e) {
      throw new DBException(ErrorType.IO_ERROR, "startNewTree failed", e);
    }
  }

  /** Split a full leaf into two siblings. Returns new right pid and its first key as push-up. */
  private <V> SplitLeafResult<K> splitLeaf(PageId leftPid, BPlusTreeLeafPage<K, V> leftLeaf)
      throws Exception {
    // Allocate right leaf
    PageId rightPid;
    BPlusTreeLeafPage<K, V> rightLeaf;
    try (WritePageGuard rg = mustNewLeafPage()) {
      rightPid = rg.getPageId();
      rightLeaf = rg.asMut(BPlusTreeLeafPage.class);
      rightLeaf.init(leafMaxSize);

      // Move half entries: keep lower half in left, move upper half to right
      final int total = leftLeaf.getSize();
      final int move = total / 2; // move upper half
      final int rightCount = total - move;

      // Copy upper half from left into right at indices [0..rightCount-1]
      for (int i = 0; i < rightCount; i++) {
        int from = move + i;
        K k = leftLeaf.keyAt(from);
        @SuppressWarnings("unchecked")
        V v = (V) leftLeaf.getRecord(from);
        rightLeaf.setKeyAt(i, k);
        rightLeaf.setRidAt(i, castRecord(v));
      }
      rightLeaf.setSize(rightCount);

      // Shrink left size
      leftLeaf.setSize(move);

      // Link siblings
      rightLeaf.setNextPageId(leftLeaf.getNextPageId());
      leftLeaf.setNextPageId(rightPid);
    }

    // Push-up key is the first key in the new right leaf
    K pushUpKey = rightLeafKeyAtZero(leftPid, rightPid);
    return new SplitLeafResult<>(pushUpKey, rightPid);
  }

  /** After creating right leaf in splitLeaf, fetch its first key safely (read-only). */
  private <V> K rightLeafKeyAtZero(PageId leftPid, PageId rightPid) {
    try (ReadPageGuard rg = mustRead(rightPid)) {
      @SuppressWarnings("unchecked")
      BPlusTreeLeafPage<K, V> right = rg.getDataAs(BPlusTreeLeafPage.class);
      return right.keyAt(0);
    } catch (Exception e) {
      throw new DBException(ErrorType.IO_ERROR, "failed to read right leaf first key", e);
    }
  }

  /** Insert separator (leftPid, pushUpKey, rightPid) into parent, splitting parents as needed. */
  private void insertIntoParent(PageId leftPid, K pushUpKey, PageId rightPid) {
    // If left is root, create a new root internal
    if (leftPid.equals(getRootPageId())) {
      try (WritePageGuard ig = mustNewInternalPage()) {
        PageId newRootPid = ig.getPageId();
        @SuppressWarnings("unchecked")
        BPlusTreeInternalPage<K> root = ig.asMut(BPlusTreeInternalPage.class);
        root.init(internalMaxSize);

        // New root has 2 children and 1 separator key at index 1
        root.setLeftmostChild(leftPid.getValue());
        root.setKeyAt(1, pushUpKey);
        root.setValueAt(1, rightPid.getValue());
        root.setSize(2); // children count

        try (WritePageGuard hg = mustWrite(headerPageId)) {
          BPlusTreeHeaderPage h = hg.asMut(BPlusTreeHeaderPage.class);
          h.setRootPageId(newRootPid);
        }
      } catch (Exception e) {
        throw new DBException(ErrorType.IO_ERROR, "insertIntoParent(new root) failed", e);
      }
      return;
    }

    // Otherwise, find parent by searching from root (since parent ptrs aren’t stored)
    PageId parentPid = findParentOf(leftPid);
    try (WritePageGuard pg = mustWrite(parentPid)) {
      @SuppressWarnings("unchecked")
      BPlusTreeInternalPage<K> parent = pg.asMut(BPlusTreeInternalPage.class);

      // Insert (key, rightPid) *after* leftPid
      int idx = valueIndex(parent, leftPid); // where leftPid sits
      // shift tail to the right to make room at (idx+1) for new child, and at key index (idx+1) for
      // pushUpKey
      insertInParentArrays(parent, idx + 1, pushUpKey, rightPid);

      // If parent overflows, split and propagate
      if (parent.getSize() > parent.getMaxSize()) {
        splitInternalAndPropagate(parentPid);
      }
    } catch (Exception e) {
      throw new DBException(ErrorType.IO_ERROR, "insertIntoParent failed", e);
    }
  }

  /** Handle a leaf underflow in a basic way (no heavy redistribution logic here). */
  private void handleLeafUnderflow(PageId leafPid) {
    // Minimal safe behavior (without parent pointers / robust redistribution):
    // Re-check root shrink; otherwise, you can leave as is or log for now.
    if (leafPid.equals(getRootPageId())) {
      try (WritePageGuard hg = mustWrite(headerPageId);
          WritePageGuard lg = mustWrite(leafPid)) {
        BPlusTreeHeaderPage h = hg.asMut(BPlusTreeHeaderPage.class);
        @SuppressWarnings("unchecked")
        BPlusTreeLeafPage<K, Object> leaf = lg.asMut(BPlusTreeLeafPage.class);
        if (leaf.getSize() == 0) {
          h.setRootPageId(PageId.store(INVALID_PAGE_ID));
        }
      } catch (Exception e) {
        throw new DBException(ErrorType.IO_ERROR, "handleLeafUnderflow failed", e);
      }
    }
    // For full BusTub behavior: borrow from siblings or merge and update parent.
    // You can implement when sibling navigation / parent search utilities are in place.
  }

  // ---------- Parent search & internal split utilities (simplified) ----------

  /** Find the parent of a child by scanning down from root (O(height * fanout)). */
  private PageId findParentOf(PageId childPid) {
    PageId root = getRootPageId();
    if (root == null || root.getValue() == INVALID_PAGE_ID) {
      throw new DBException(ErrorType.INDEX_NOT_FOUND, "Tree empty; no parent for " + childPid);
    }
    if (root.equals(childPid)) {
      throw new DBException(ErrorType.INDEX_NOT_FOUND, "Root has no parent");
    }

    PageId pid = root;
    while (true) {
      try (ReadPageGuard rg = mustRead(pid)) {
        BPlusTreePage hdr = rg.getDataAs(BPlusTreePage.class);
        if (hdr.isLeafPage()) break;

        @SuppressWarnings("unchecked")
        BPlusTreeInternalPage<K> internal = rg.getDataAs(BPlusTreeInternalPage.class);
        // Scan children at this internal
        for (int i = 0; i < internal.getSize(); i++) {
          PageId cpid = internal.valueAt(i);
          if (cpid.equals(childPid)) {
            return pid; // found parent
          }
        }
        // Not here; descend toward the subtree that would contain childPid by using *keys*, but
        // since we don't have child's min key, do a full scan over children; pick some child to
        // descend.
        // Heuristic: descend leftmost; then the loop will eventually find the parent as we scan
        // levels.
        pid = internal.valueAt(0);
      } catch (Exception e) {
        throw new DBException(ErrorType.IO_ERROR, "findParentOf failed", e);
      }
    }
    throw new DBException(ErrorType.INDEX_NOT_FOUND, "Parent not found for " + childPid);
  }

  /** Locate the index of a child pid inside an internal page's children array. */
  private int valueIndex(BPlusTreeInternalPage<K> node, PageId childPid) {
    for (int i = 0; i < node.getSize(); i++) {
      if (node.valueAt(i).equals(childPid)) return i;
    }
    throw new DBException(ErrorType.INDEX_NOT_FOUND, "Child not found in parent");
  }

  /** Insert (key,rightPid) at child position 'at' in parent; shifts arrays and bumps size. */
  private void insertInParentArrays(
      BPlusTreeInternalPage<K> parent, int at, K key, PageId rightPid) {
    final int size = parent.getSize();
    if (size >= parent.getMaxSize()) {
      // We’ll still try to shift; split will happen after this insertion.
    }

    // shift children [at..size-1] → [at+1..]
    for (int i = size; i > at; i--) {
      parent.setValueAt(i, parent.valueAt(i - 1).getValue());
    }
    // shift keys [at..size-1] → [at+1..] (keys are at 1..size-1; we allow writing at range up to
    // maxSize-1)
    for (int i = size; i > at; i--) {
      if (i >= 1) {
        parent.setKeyAt(i, parent.keyAt(i - 1));
      }
    }

    // place new entries
    if (at >= 1) parent.setKeyAt(at, key);
    parent.setValueAt(at, rightPid.getValue());

    parent.setSize(size + 1); // number of children increased by 1
  }

  /** Split an internal page at pid and propagate middle key up. */
  private void splitInternalAndPropagate(PageId leftPid) {
    // Allocate right internal
    PageId rightPid;
    try (WritePageGuard lg = mustWrite(leftPid);
        WritePageGuard rg = mustNewInternalPage()) {

      rightPid = rg.getPageId();
      @SuppressWarnings("unchecked")
      BPlusTreeInternalPage<K> left = lg.asMut(BPlusTreeInternalPage.class);
      @SuppressWarnings("unchecked")
      BPlusTreeInternalPage<K> right = rg.asMut(BPlusTreeInternalPage.class);

      right.init(internalMaxSize);

      // Move half (children/keys) to right and compute middle key to push up
      final int size = left.getSize();
      final int mid = size / 2; // child index to split around; push-up key sits at index mid
      // Right gets children [mid..size-1], and keys [mid..size-1] (remember keys start at 1)

      // First key to push up is keyAt(mid) (mid >= 1 when size >= 2)
      K pushUp = left.keyAt(mid);

      // Copy children & keys to right
      int rSize = 0;
      for (int i = mid; i < size; i++) {
        right.setValueAt(rSize, left.valueAt(i).getValue());
        if (i >= 1) right.setKeyAt(rSize, left.keyAt(i));
        rSize++;
      }
      right.setSize(rSize);

      // Shrink left to size = mid
      left.setSize(mid);

      // Propagate into parent
      insertIntoParent(leftPid, pushUp, rightPid);

    } catch (Exception e) {
      throw new DBException(ErrorType.IO_ERROR, "splitInternalAndPropagate failed", e);
    }
  }

  // ------------------------- Guard + BPM helpers -------------------------

  private ReadPageGuard mustRead(PageId pid) {
    Optional<ReadPageGuard> g = bufferPoolManager.checkedReadPage(pid);
    if (g.isEmpty()) throw new DBException(ErrorType.IO_ERROR, "Unable to read page " + pid);
    return g.get();
  }

  private WritePageGuard mustWrite(PageId pid) {
    Optional<WritePageGuard> g = bufferPoolManager.checkedPageWrite(pid);
    if (g.isEmpty()) throw new DBException(ErrorType.IO_ERROR, "Unable to write page " + pid);
    return g.get();
  }

  /** Allocate a brand-new page and view it as a **leaf**. Adapt to your BPM API. */
  private WritePageGuard mustNewLeafPage() {
    // >>> Adapt this to your BufferPoolManager <<<
    // e.g., Optional<WritePageGuard> g = bufferPoolManager.newPageWrite();
    Optional<WritePageGuard> g = bufferPoolManager.newPage(); // <-- implement this in BPM
    if (g.isEmpty()) throw new DBException(ErrorType.IO_ERROR, "Unable to allocate new leaf page");
    return g.get();
  }

  /** Allocate a brand-new page and view it as an **internal**. Adapt to your BPM API. */
  private WritePageGuard mustNewInternalPage() {
    Optional<WritePageGuard> g = bufferPoolManager.newPage(); // <-- implement this in BPM
    if (g.isEmpty())
      throw new DBException(ErrorType.IO_ERROR, "Unable to allocate new internal page");
    return g.get();
  }

  private <V> com.dbms.yadbms.storage.page.RecordId castRecord(V v) {
    // Leaf APIs store V as RecordId
    return (com.dbms.yadbms.storage.page.RecordId) v;
  }

  private static final class SplitLeafResult<K> {
    final K pushUpKey;
    final PageId newRightPid;

    SplitLeafResult(K key, PageId pid) {
      this.pushUpKey = key;
      this.newRightPid = pid;
    }
  }
}
