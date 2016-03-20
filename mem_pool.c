/*
 * Created by Ivo Georgiev on 2/9/16.
 * Modified by Gabriella Ramirez
 */

#include <stdlib.h>
#include <assert.h>
#include <stdio.h> // for perror()

#include "mem_pool.h"

/*************/
/*           */
/* Constants */
/*           */
/*************/
static const float      MEM_FILL_FACTOR                 = 0.75;
static const unsigned   MEM_EXPAND_FACTOR               = 2;

static const unsigned   MEM_POOL_STORE_INIT_CAPACITY    = 20;
static const float      MEM_POOL_STORE_FILL_FACTOR      = 0.75;
static const unsigned   MEM_POOL_STORE_EXPAND_FACTOR    = 2;

static const unsigned   MEM_NODE_HEAP_INIT_CAPACITY     = 40;
static const float      MEM_NODE_HEAP_FILL_FACTOR       = 0.75;
static const unsigned   MEM_NODE_HEAP_EXPAND_FACTOR     = 2;

static const unsigned   MEM_GAP_IX_INIT_CAPACITY        = 40;
static const float      MEM_GAP_IX_FILL_FACTOR          = 0.75;
static const unsigned   MEM_GAP_IX_EXPAND_FACTOR        = 2;



/*********************/
/*                   */
/* Type declarations */
/*                   */
/*********************/
typedef struct _node {
    alloc_t alloc_record;
    unsigned used;
    unsigned allocated;
    struct _node *next, *prev; // doubly-linked list for gap deletion
} node_t, *node_pt;

typedef struct _gap {
    size_t size;
    node_pt node;
} gap_t, *gap_pt;

typedef struct _pool_mgr {
    pool_t pool;
    node_pt node_heap;
    unsigned total_nodes;
    unsigned used_nodes;
    gap_pt gap_ix;
    unsigned gap_ix_capacity;
} pool_mgr_t, *pool_mgr_pt;



/***************************/
/*                         */
/* Static global variables */
/*                         */
/***************************/
static pool_mgr_pt *pool_store = NULL; // an array of pointers, only expand
static unsigned pool_store_size = 0;
static unsigned pool_store_capacity = 0;



/********************************************/
/*                                          */
/* Forward declarations of static functions */
/*                                          */
/********************************************/
static alloc_status _mem_resize_pool_store();
static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr);
static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr);
static alloc_status
        _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                           size_t size,
                           node_pt node);
static alloc_status
        _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                size_t size,
                                node_pt node);
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr);



/****************************************/
/*                                      */
/* Definitions of user-facing functions */
/*                                      */
/****************************************/
alloc_status mem_init() {
    // ensure that it's called only once until mem_free
    // allocate the pool store with initial capacity
    // note: holds pointers only, other functions to allocate/deallocate

	if(pool_store == NULL)
	{
		pool_store = calloc(MEM_POOL_STORE_INIT_CAPACITY, sizeof(pool_mgr_pt));

		if(pool_store == NULL)
		{
			return ALLOC_FAIL;
		}

		else
		{
			pool_store_capacity = MEM_POOL_STORE_INIT_CAPACITY;
			pool_store_size = 0;

			return ALLOC_OK;
		}
	}

	else
	{
		return ALLOC_CALLED_AGAIN;
	}

    return ALLOC_FAIL;
}

alloc_status mem_free() {
    // ensure that it's called only once for each mem_init
    // make sure all pool managers have been deallocated
    // can free the pool store array
    // update static variables

	if(pool_store != NULL)
	{
		free(pool_store);
		pool_store = NULL;
		pool_store_capacity = 0;
		pool_store_size = 0;

		return ALLOC_OK;
	}

	else
	{
		return ALLOC_CALLED_AGAIN;
	}
}

pool_pt mem_pool_open(size_t size, alloc_policy policy) {
    // make sure there the pool store is allocated
    // expand the pool store, if necessary
    // allocate a new mem pool mgr
    // check success, on error return null
    // allocate a new memory pool
    // check success, on error deallocate mgr and return null
    // allocate a new node heap
    // check success, on error deallocate mgr/pool and return null
    // allocate a new gap index
    // check success, on error deallocate mgr/pool/heap and return null
    // assign all the pointers and update meta data:
    //   initialize top node of node heap
    //   initialize top node of gap index
    //   initialize pool mgr
    //   link pool mgr to pool store
    // return the address of the mgr, cast to (pool_pt)

    if(pool_store == NULL)
	{
		alloc_status call_status;
		call_status = mem_init();

		if(call_status == ALLOC_FAIL)
		{
			return NULL;
		}
	}

	else
	{
		_mem_resize_pool_store();

		pool_mgr_pt new_pool_mgr;
		new_pool_mgr = calloc(1, sizeof(pool_mgr_t));

		if(new_pool_mgr == NULL)
		{
			return NULL;
		}

		//Allocate New Memory Pool
		new_pool_mgr->pool.mem = malloc(size);

		if(new_pool_mgr->pool.mem == NULL)
		{
			free(new_pool_mgr);

			return NULL;
		}

		//Initialize Memory Pool
		new_pool_mgr->pool.policy = policy;
		new_pool_mgr->pool.total_size = size;
		new_pool_mgr->pool.alloc_size = 0;
		new_pool_mgr->pool.num_allocs = 0;
		new_pool_mgr->pool.num_gaps = 1;

		//Allocate New Node Heap
		new_pool_mgr->node_heap = calloc(MEM_NODE_HEAP_INIT_CAPACITY, sizeof(node_t));
        new_pool_mgr->total_nodes = MEM_NODE_HEAP_INIT_CAPACITY;

        if (new_pool_mgr->node_heap == NULL)
        {
            free(new_pool_mgr->pool.mem);
            free(new_pool_mgr);

            return NULL;
        }

        //Allocate New Gap Index
        new_pool_mgr->gap_ix = calloc(MEM_GAP_IX_INIT_CAPACITY, sizeof(gap_t));
        new_pool_mgr->gap_ix_capacity = MEM_GAP_IX_INIT_CAPACITY;

        if (new_pool_mgr->gap_ix == NULL)
        {
            free(new_pool_mgr->node_heap);
            free(&new_pool_mgr->pool);
            free(new_pool_mgr);

            return NULL;
        }

		//Initialize Top Node of Node Heap
        new_pool_mgr->node_heap->prev = NULL;
        new_pool_mgr->node_heap->next = NULL;
        new_pool_mgr->node_heap->used = 0;
        new_pool_mgr->node_heap->allocated = 0;
        new_pool_mgr->node_heap->alloc_record.mem = new_pool_mgr->pool.mem;
        new_pool_mgr->node_heap->alloc_record.size = size;

        //Initialize Top Node of Gap Index
        new_pool_mgr->gap_ix->node = new_pool_mgr->node_heap;
        new_pool_mgr->gap_ix->size = size;

		//Initialize new_pool_mgr
        new_pool_mgr->pool.policy = policy;
        new_pool_mgr->pool.total_size = size;
        new_pool_mgr->pool.alloc_size = 0;
        new_pool_mgr->pool.num_allocs = 0;
        new_pool_mgr->pool.num_gaps = 1;
        new_pool_mgr->used_nodes = 1;

		//Link new_pool_mgr to pool_store
        pool_store[pool_store_size] = new_pool_mgr;
        pool_store_size = 1;

        return (pool_pt) new_pool_mgr;
	}
}

alloc_status mem_pool_close(pool_pt pool) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    // check if this pool is allocated
    // check if pool has only one gap
    // check if it has zero allocations
    // free memory pool
    // free node heap
    // free gap index
    // find mgr in pool store and set to null
    // note: don't decrement pool_store_size, because it only grows
    // free mgr

    pool_mgr_pt new_pool_mgr = (pool_mgr_pt) pool;
	unsigned counter = 0;

	if(new_pool_mgr != NULL)
	{
		for(unsigned index = 0; index < new_pool_mgr->gap_ix_capacity; index++)
		{
			if((new_pool_mgr->gap_ix)[index].size > 0)
			{
				counter++;
			}
		}

		if(counter == 1 && pool->num_allocs == 0)
		{
			for(int index1 = 0; index1 < pool_store_capacity; index1++)
			{
				if(pool_store[index1] == new_pool_mgr)
				{
					free((new_pool_mgr->pool).mem);
					free(new_pool_mgr->node_heap);
					free(new_pool_mgr->gap_ix);

					pool_store[index1] == NULL;

					break;
				}
			}

			return ALLOC_OK;
		}

		else
		{
			return ALLOC_NOT_FREED;
		}
	}

	return ALLOC_FAIL;
}

alloc_pt mem_new_alloc(pool_pt pool, size_t size) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    // check if any gaps, return null if none
    // expand heap node, if necessary, quit on error
    // check used nodes fewer than total nodes, quit on error
    // get a node for allocation:
    // if FIRST_FIT, then find the first sufficient node in the node heap
    // if BEST_FIT, then find the first sufficient node in the gap index
    // check if node found
    // update metadata (num_allocs, alloc_size)
    // calculate the size of the remaining gap, if any
    // remove node from gap index
    // convert gap_node to an allocation node of given size
    // adjust node heap:
    //   if remaining gap, need a new node
    //   find an unused one in the node heap
    //   make sure one was found
    //   initialize it to a gap node
    //   update metadata (used_nodes)
    //   update linked list (new node right after the node for allocation)
    //   add to gap index
    //   check if successful
    // return allocation record by casting the node to (alloc_pt)

    pool_mgr_pt new_pool_mgr = (pool_mgr_pt) pool;
    assert(new_pool_mgr->used_nodes < new_pool_mgr->total_nodes);

    if(new_pool_mgr->gap_ix == NULL)
    {
        return NULL;
    }

    node_pt new_node;
    int index = 0;

    if(new_pool_mgr->pool.policy == FIRST_FIT)
    {
        node_pt heap = new_pool_mgr->node_heap;

        while(((new_pool_mgr->node_heap[index].alloc_record.size) < size) || ((new_pool_mgr->node_heap[index].allocated != 0) && (new_pool_mgr->node_heap[index].used !=0)))
        {
            if((&new_pool_mgr->node_heap[index] == new_pool_mgr->gap_ix[0].node) && ((new_pool_mgr->node_heap[index].alloc_record.size) < size))
            {
                return NULL;
            }

            index += 1;
        }

        if(heap == NULL)
        {
            return NULL;
        }

        new_node = &new_pool_mgr->node_heap[index];
    }

    else if(new_pool_mgr->pool.policy == BEST_FIT)
    {
        if(new_pool_mgr->pool.num_gaps > 0)
        {
            while((index < new_pool_mgr->pool.num_gaps) && ((new_pool_mgr->gap_ix[index + 1].size) >= size))
            {
                index += 1;
            }
        }

        else
        {
            return NULL;
        }
        new_node = new_pool_mgr->gap_ix[index].node;
    }

    if(new_node == NULL)
    {
        return NULL;
    }

    new_pool_mgr->pool.alloc_size += size;
    new_pool_mgr->pool.num_allocs += 1;

    size_t remaining = 0;

    if((new_node->alloc_record.size - size) > 0)
    {
        remaining = new_node->alloc_record.size - size;
    }

    _mem_remove_from_gap_ix(new_pool_mgr, size, new_node);

    new_node->allocated = 1;
    new_node->used = 1;
    new_node->alloc_record.size = size;

    if(remaining != 0)
    {
        int index = 0;
        while(new_pool_mgr->node_heap[index].used != 0)
        {
            index += 1;
        }
        node_pt new_gap = &new_pool_mgr->node_heap[index];

        new_gap->used = 1;
        new_gap->allocated = 0;
        new_gap->alloc_record.size = remaining;

        new_pool_mgr->used_nodes += 1;

        new_node->alloc_record.size = size;

        if(new_node->next)
        {
            new_node->next->prev = new_gap;
        }

        new_gap->next = new_node->next;
        new_node->next = new_gap;
        new_gap->prev = new_node;

        _mem_add_to_gap_ix(new_pool_mgr, remaining, new_gap);
    }

    return (alloc_pt) new_node;
}

alloc_status mem_del_alloc(pool_pt pool, alloc_pt alloc) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    // get node from alloc by casting the pointer to (node_pt)
    // find the node in the node heap
    // this is node-to-delete
    // make sure it's found
    // convert to gap node
    // update metadata (num_allocs, alloc_size)
    // if the next node in the list is also a gap, merge into node-to-delete
    //   remove the next node from gap index
    //   check success
    //   add the size to the node-to-delete
    //   update node as unused
    //   update metadata (used nodes)
    //   update linked list:
    /*
                    if (next->next) {
                        next->next->prev = node_to_del;
                        node_to_del->next = next->next;
                    } else {
                        node_to_del->next = NULL;
                    }
                    next->next = NULL;
                    next->prev = NULL;
     */

    // this merged node-to-delete might need to be added to the gap index
    // but one more thing to check...
    // if the previous node in the list is also a gap, merge into previous!
    //   remove the previous node from gap index
    //   check success
    //   add the size of node-to-delete to the previous
    //   update node-to-delete as unused
    //   update metadata (used_nodes)
    //   update linked list
    /*
                    if (node_to_del->next) {
                        prev->next = node_to_del->next;
                        node_to_del->next->prev = prev;
                    } else {
                        prev->next = NULL;
                    }
                    node_to_del->next = NULL;
                    node_to_del->prev = NULL;
     */
    //   change the node to add to the previous node!
    // add the resulting node to the gap index
    // check success

    pool_mgr_pt new_pool_mgr = (pool_mgr_pt) pool;
    node_pt node = (node_pt) alloc;
    node_pt node_to_delete = (node_pt) alloc;

    for(int index = 0; index < new_pool_mgr->total_nodes; index++)
    {
        if(node == &new_pool_mgr->node_heap[index])
        {
            node_to_delete = &new_pool_mgr->node_heap[index];
            break;
        }
    }

    if(node_to_delete == NULL)
    {
        return ALLOC_FAIL;
    }

    if((node_to_delete->next != NULL) && (node_to_delete->next->allocated == 0))
    {
        node_pt next = node_to_delete->next;
        if(_mem_remove_from_gap_ix(new_pool_mgr, 0, next) == ALLOC_FAIL)
        {
            return ALLOC_FAIL;
        }

        node_to_delete->alloc_record.size += next->alloc_record.size;
        next->used = 0;
        new_pool_mgr->used_nodes -= 1;

        if(next->next)
        {
            next->next->prev = node_to_delete;
            node_to_delete->next = next->next;
        }

        else
        {
            node_to_delete->next = NULL;
        }

        next->next = NULL;
        next->prev = NULL;
    }

    if(node_to_delete->prev != NULL && node_to_delete->prev->allocated == 0)
    {
        node_pt previous = node_to_delete->prev;
        if(_mem_remove_from_gap_ix(new_pool_mgr, 0, previous) == ALLOC_FAIL)
        {
            return ALLOC_FAIL;
        }

        previous->alloc_record.size += node_to_delete->alloc_record.size;
        node_to_delete->used = 0;
        new_pool_mgr->used_nodes -= 1;
        if(node_to_delete->next)
        {
            previous->next = node_to_delete->next;
            node_to_delete->next->prev = previous;
        }

        else
        {
            previous->next = NULL;
        }

        node_to_delete = previous;
    }

    if(_mem_add_to_gap_ix(new_pool_mgr, node_to_delete->alloc_record.size, node_to_delete) != ALLOC_OK)
    {
        return ALLOC_FAIL;
    }

    else
    {
        return ALLOC_OK;
    }
}

void mem_inspect_pool(pool_pt pool,
                      pool_segment_pt *segments,
                      unsigned *num_segments) {
    // get the mgr from the pool
    // allocate the segments array with size == used_nodes
    // check successful
    // loop through the node heap and the segments array
    //    for each node, write the size and allocated in the segment
    // "return" the values:
    /*
                    *segments = segs;
                    *num_segments = pool_mgr->used_nodes;
     */

     pool_mgr_pt new_pool_mgr = (pool_mgr_pt) pool;
     node_pt node = new_pool_mgr->node_heap;
     *num_segments = new_pool_mgr->used_nodes;
     *segments = (pool_segment_pt) calloc (((pool_mgr_pt)pool)->used_nodes, sizeof(pool_segment_t));

     if(*segments != NULL)
     {
        for(int index = 0; index < new_pool_mgr->used_nodes; index++)
        {
            (*segments)[index].allocated = node->allocated;
            (*segments)[index].size = ((alloc_pt)node)->size;

            node = node->next;
        }
     }
}



/***********************************/
/*                                 */
/* Definitions of static functions */
/*                                 */
/***********************************/
static alloc_status _mem_resize_pool_store() {
    // check if necessary
    /*
                if (((float) pool_store_size / pool_store_capacity)
                    > MEM_POOL_STORE_FILL_FACTOR) {...}
     */
    // don't forget to update capacity variables

    unsigned new_pool_store_capacity;

    if (pool_store_size == pool_store_capacity)
    {
        new_pool_store_capacity = MEM_EXPAND_FACTOR * pool_store_capacity;

        pool_store = realloc(pool_store, new_pool_store_capacity);

        if (pool_store == NULL)
        {
           return ALLOC_FAIL;
        }

        pool_store_capacity = new_pool_store_capacity;
    }

    else if (pool_store_size != pool_store_capacity)
    {
        return ALLOC_OK;
    }
}

static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr) {
    // see above

    unsigned new_node_count;
    node_pt new_node_heap;

    if(((float)pool_mgr->used_nodes/pool_mgr->total_nodes) > MEM_NODE_HEAP_FILL_FACTOR)
    {
        new_node_count = pool_mgr->total_nodes * MEM_NODE_HEAP_EXPAND_FACTOR;
        new_node_heap = (node_pt)realloc(pool_mgr->node_heap, new_node_count);

        if(new_node_heap != NULL)
        {
            pool_mgr->total_nodes = new_node_count;
            pool_mgr->node_heap = new_node_heap;

            return ALLOC_OK;
        }
    }

    else
    {
        return ALLOC_OK;
    }

    return ALLOC_FAIL;
}

static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr) {
    // see above

    unsigned new_gap_capacity;

    if ((pool_mgr->pool.num_gaps/pool_mgr->gap_ix_capacity) > MEM_GAP_IX_FILL_FACTOR)
    {
        new_gap_capacity = (sizeof(pool_mgr->gap_ix) * MEM_GAP_IX_EXPAND_FACTOR);

        pool_mgr->gap_ix = realloc(pool_mgr->gap_ix, new_gap_capacity);
        pool_mgr->gap_ix_capacity = pool_mgr->gap_ix_capacity * MEM_GAP_IX_EXPAND_FACTOR;

        return ALLOC_OK;
    }

    else
    {
        return ALLOC_FAIL;
    }
}

static alloc_status _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                                       size_t size,
                                       node_pt node) {

    // expand the gap index, if necessary (call the function)
    // add the entry at the end
    // update metadata (num_gaps)
    // sort the gap index (call the function)
    // check success

    _mem_resize_gap_ix(pool_mgr);

    int index = 0;

    while(pool_mgr->gap_ix[index].node != NULL)
    {
        index += 1;
    }

    pool_mgr->gap_ix[index].node = node;
    pool_mgr->gap_ix[index].size = size;

    pool_mgr->pool.num_gaps++;

    if(pool_mgr->gap_ix[index].node != NULL)
    {
        _mem_sort_gap_ix(pool_mgr);
        return ALLOC_OK;
    }

    else
    {
        return ALLOC_FAIL;
    }
}

static alloc_status _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                            size_t size,
                                            node_pt node) {
    // find the position of the node in the gap index
    // loop from there to the end of the array:
    //    pull the entries (i.e. copy over) one position up
    //    this effectively deletes the chosen node
    // update metadata (num_gaps)
    // zero out the element at position num_gaps!

    gap_pt new_gap = pool_mgr->gap_ix;
    int location = -1;

    for(int index = 0; index < pool_mgr->gap_ix_capacity; index++)
    {
        if((new_gap[index].size == size) && (((new_gap[index].node)->alloc_record).mem == (node->alloc_record).mem))
        {
            location = index;
            break;
        }
    }

    if(location == -1)
    {
        return ALLOC_FAIL;
    }

    else
    {
        for(int index1 = location; index1 < (pool_mgr->gap_ix_capacity - 1); index1++)
        {
            new_gap[index1] = new_gap[index1 + 1];
        }

        new_gap[pool_mgr->gap_ix_capacity - 1].size = 0;
        new_gap[pool_mgr->gap_ix_capacity - 1].node = NULL;

        return ALLOC_OK;
    }
}

// note: only called by _mem_add_to_gap_ix, which appends a single entry
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr) {
    // the new entry is at the end, so "bubble it up"
    // loop from num_gaps - 1 until but not including 0:
    //    if the size of the current entry is less than the previous (u - 1)
    //    or if the sizes are the same but the current entry points to a
    //    node with a lower address of pool allocation address (mem)
    //       swap them (by copying) (remember to use a temporary variable)

    gap_pt new_gap = pool_mgr->gap_ix;
    gap_t temp_gap;

    if(new_gap)
    {
        for(int index = pool_mgr->pool.num_gaps - 1; index > 0; index++)
        {
            if((new_gap[index].size < new_gap[index - 1].size && new_gap[index].size != 0 ) || (new_gap[index].size > new_gap[index - 1].size && new_gap[index-1].size == 0 ) || (new_gap[index].size == new_gap[index-1].size && &((new_gap[index].node)->alloc_record).mem < &((new_gap[index-1].node)->alloc_record).mem))
            {
                temp_gap = new_gap[index];
                new_gap[index] = new_gap[index - 1];
                new_gap[index - 1] = temp_gap;
            }
        }

        return ALLOC_OK;
    }

    else
    {
        return ALLOC_FAIL;
    }
}


