// query.c ... query scan functions
// part of Multi-attribute Linear-hashed Files
// Manage creating and using Query objects
// Written by John Shepherd, April 2016

#include "defs.h"
#include "query.h"
#include "reln.h"
#include "tuple.h"
#include "hash.h"
#include "bits.h"
#include "chvec.h"
#include "page.h"

// A suggestion ... you can change however you like

struct QueryRep {
	Reln    rel;       // need to remember Relation info
	int    known[32];     // the hash value from MAH
	int    unknown[32];   // the unknown bits from MAH
	int num_of_unknown;
	char hashBit[33];
	PageID  curpage;   // current page in scan
	int     is_ovflow; // are we in the overflow pages?
	Offset  curtup;    // offset of current tuple within page
	int total_step;
	int current_step;
	int num_of_scan;
	char *c;
	Page pg;
	char *target;
	int nAttrs;
	Count pageTuple;
	Offset sp;
	int is_sp;
	//TODO
};

// take a query string (e.g. "1234,?,abc,?")
// set up a QueryRep object for the scan

Query startQuery(Reln r, char *q)
{
	Query new = malloc(sizeof(struct QueryRep));
	assert(new != NULL);
	// TODO
	// Partial algorithm:
	// form known bits from known attributes
	// form unknown bits from '?' attributes
	// compute PageID of first page
	//   using known bits and first "unknown" value
	// set all values in QueryRep object
	
	//initialise
	char *c; int nf = 1;
	for (c = q; *c != '\0'; c++)
		if (*c == ',') nf++;
	// invalid tuple
	if (nf != nattrs(r)) return NULL;
	new->nAttrs = nattrs(r);
	new->target = q;
	new->rel = r;
	Count nvals = nattrs(r);
	ChVecItem *cv = chvec(r);
	char **vals = malloc(nvals*sizeof(char *));
	Bits hash[nvals];
	new->sp = splitp(r);
	// char buf[40];

	assert(vals != NULL);
	tupleVals(q, vals);
	// printf("%s\n", vals[0]);
	// for (int i = 0; i < MAXCHVEC; i++) {
	// 	printf("%d,%d",cv[i].att, cv[i].bit);
	// 	if (i < MAXCHVEC-1) putchar(':');
	// }
	// printf("\n");
	Count dep = depth(r);
	// printf("depth: %d \n", dep);
 	// char buf[40];
	for (int i = 0; i< nvals; i++) {
		// if (vals[i] == NULL) return NULL;
		if(strcmp(vals[i],"?") != 0) {
			hash[i] = hash_any((unsigned char *)vals[i],strlen(vals[i]));
			// showBits(hash[i], buf);
			// printf("hash(%s) = %s\n",vals[i], buf);
			for (int j = 0; j < dep;j++) {
				if (cv[j].att == i) {
					new->hashBit[dep - j - 1] = bitIsSet(hash[i], cv[j].bit) + 48;
				}
			}
		} else {
			// printf("#%d: %s\n", i, vals[i]);
			for (int j = 0; j < dep;j++) {
				if (cv[j].att == i) {
					new->hashBit[dep - j - 1] = '?';
				}
			}
		}
	}
	new->hashBit[dep] = '\0';
	// printf("hash bit: %s\n", new->hashBit);
	//record the position of '?'
	new->num_of_unknown = 0;
	for (int i = dep - 1; i>= 0; i--) {
		if(new->hashBit[i] == '?') {
			new->unknown[new->num_of_unknown] = i;
			new->num_of_unknown++;
			// printf("Position: %d, num_of_unknown: %d\n", i, new->num_of_unknown);
		}
	}
	
	new->total_step = (1<<(new->num_of_unknown));
	// printf("Total step: %d\n", new->total_step);
	new->current_step = 0;

	//set up current bits
	new->curpage = 0;
	for (int i = 0;i < dep;i++) {
		if (new->hashBit[i] == '1') {
			new->curpage = setBit(new->curpage, dep - i - 1);
		} else{
			new->curpage = unsetBit(new->curpage, dep - i - 1);
		}
	}
	getPageID(new, new->current_step);
	free(vals);
	return new;
}

// get next tuple during a scan

Tuple getNextTuple(Query q)
{
	// TODO
	// Partial algorithm:
	// if (more tuples in current page)
	//    get next matching tuple from current page
	// else if (current page has overflow)
	//    move to overflow page
	//    grab first matching tuple from page
	// else
	//    move to "next" bucket
	//    grab first matching tuple from data page
	// endif
	// if (current page has no matching tuples)
	//    go to next page (try again)
	// endif
	
	// char *c = getStartData(q->pg);
	// printf("%s\n", c);
	// Count pageTuple = pageTuples(q->pg); 
	while(1) {
		while (q->num_of_scan < q->pageTuple) {
			// c = c + q->lengthAdd;
			q->num_of_scan++;
			// q->lengthAdd = q->lengthAdd + strlen(c) + 1;
			if (tupleMatch(q->target,q->c,q->nAttrs) == TRUE) {
				char *goodResult = q->c;
				q->c = q->c + strlen(q->c) + 1;

				return goodResult;
			} else {
				q->c = q->c + strlen(q->c) + 1;
			}
		}
		if (q->is_ovflow != NO_PAGE) {
			q->num_of_scan = 0;
			
			q->curpage = pageOvflow(q->pg);
			if(q->pg != NULL) free(q->pg);
			q->pg = getPage(getOverflow(q->rel),q->curpage);
			// q->lengthAdd = 0;
			q->c = getStartData(q->pg);
			q->pageTuple = pageTuples(q->pg);
			q->is_ovflow = pageOvflow(q->pg);
			// printf("\nStep %d, overflow page, num of tuples: %d\n", q->current_step, q->pageTuple);
			continue;

		}
		if (q->is_sp == 1) {
			q->is_sp = 0;
			q->num_of_scan = 0;
			Count dep = depth(q->rel);
			q->curpage = q->curpage + (1<<dep);
			if(q->pg != NULL) free(q->pg);
			q->pg = getPage(getData(q->rel),q->curpage);
			q->c = getStartData(q->pg);
			q->is_ovflow = pageOvflow(q->pg);
			q->pageTuple = pageTuples(q->pg);
			// printf("\nStep %d, split page, num of tuples: %d\n", q->current_step, q->pageTuple);

			continue;
		}

		if (q->current_step < q->total_step - 1 ) {
			q->current_step++;
			// q->num_of_scan = 0;
			// q->lengthAdd = 0;
			getPageID(q, q->current_step);
			// q->c = getStartData(q->pg);
			// pageTuple = pageTuples(q->pg); 
		}
		if ((q->is_ovflow == NO_PAGE) && q->current_step >= q->total_step - 1 && q->num_of_scan == q->pageTuple) {
			break;
		}
	}
	
	




	return NULL;
}

// clean up a QueryRep object and associated data

void closeQuery(Query q)
{
	// TODO
	free(q);
}


void getPageID(Query q, int step) {

	// q->curpage = 0;
	// printf("Original page Id: %d\n", q->curpage);
	Count dep = depth(q->rel);
	if (q->curpage >= (1<<dep)) {
		q->curpage = q->curpage - (1<<dep);
	}
	if(q->pg != NULL) free(q->pg);
	// Count dep = depth(q->rel);
	for (int i = 0; i< q->num_of_unknown; i++) {
		int cur_bit = bitIsSet(step, i);
		// printf("current bit: %d\n", cur_bit);
		if (cur_bit == 0) {
			q->curpage = unsetBit(q->curpage, dep - q->unknown[i] - 1);
		} else {
			q->curpage = setBit(q->curpage, dep - q->unknown[i] - 1);
		}
	}
	q->num_of_scan = 0;
	
	// printf("q->curpage: %d\n", q->curpage);
	// Reln r = q-> rel;
	q->pg = getPage(getData(q->rel),q->curpage);
	q->c = getStartData(q->pg);
	q->is_ovflow = pageOvflow(q->pg);
	q->pageTuple = pageTuples(q->pg);
	if (q->curpage < q->sp) {
		q->is_sp = 1;
	} else {
		q->is_sp = 0;
	}
	// printf("\nStep %d, normal page ID: %d, num of tuples: %d\n", q->current_step, q->curpage, q->pageTuple);
	
}
