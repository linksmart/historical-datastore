package common

import (
	"fmt"
	"math"
	"strconv"
)

func ParsePagingParams(page, perPage string, maxPerPage int) (int, int, error) {
	var parsedPage, parsedPerPage int
	var err error

	if page == "" {
		parsedPage = 1
	} else {
		parsedPage, err = strconv.Atoi(page)
		if err != nil {
			return 0, 0, fmt.Errorf("Invalid value for parameter %s: %s", ParamPage, page)
		}
	}

	if perPage == "" {
		parsedPerPage = 100
	} else {
		parsedPerPage, err = strconv.Atoi(perPage)
		if err != nil {
			return 0, 0, fmt.Errorf("Invalid value for parameter %s: %s", ParamPerPage, perPage)
		}
	}

	return parsedPage, parsedPerPage, ValidatePagingParams(parsedPage, parsedPerPage, maxPerPage)
}

func ValidatePagingParams(page, perPage, maxPerPage int) error {
	if page < 1 {
		return fmt.Errorf("%s number must be greater than or equal to 1", ParamPage)
	}
	if perPage < 1 {
		return fmt.Errorf("%s must be greater than or equal to 1", ParamPerPage)
	}
	if perPage > maxPerPage {
		return fmt.Errorf("%s must be less than or equal to max(%d)", ParamPerPage, maxPerPage)
	}
	return nil
}

// Check if the parameters match the criteria required by PerItemPagination function
func ValidatePerItemLimit(_qLimit, _perPage, _numOfSrcs int) error {
	qLimit, perPage, numOfSrcs := float64(_qLimit), float64(_perPage), float64(_numOfSrcs)

	if qLimit == 0 {
		return fmt.Errorf("%v must be positive", ParamLimit)
	}

	// qLimit must be divisible by the number of sources
	if qLimit > 0 && math.Remainder(qLimit, numOfSrcs) != 0 {
		qLimit = math.Ceil(qLimit/numOfSrcs) * numOfSrcs
		return fmt.Errorf("%v must be divisible by the number of queried sources. E.g. %v=%v", ParamLimit, ParamLimit, qLimit)
	}

	// perPage must be divisible by the number of sources
	if math.Remainder(perPage, numOfSrcs) != 0 {
		perPage = math.Ceil(perPage/numOfSrcs) * numOfSrcs
		return fmt.Errorf("%v must be divisible by the number of queried sources. E.g. %v=%v", ParamPerPage, ParamPerPage, perPage)
	}

	return nil
}

// Calculate perItem and offset given the page, perPage, limit, and number of sources
func PerItemPagination(_qLimit, _page, _perPage, _numOfSrcs int) ([]int, []int) {
	qLimit, page, perPage, numOfSrcs := float64(_qLimit), float64(_page), float64(_perPage), float64(_numOfSrcs)
	limitIsSet := qLimit > 0
	page-- // make page number 0-indexed

	// Make qLimit and perPage divisible by the number of sources
	if limitIsSet && math.Remainder(qLimit, numOfSrcs) != 0 {
		qLimit = math.Ceil(qLimit/numOfSrcs) * numOfSrcs
	}
	if math.Remainder(perPage, numOfSrcs) != 0 {
		perPage = math.Ceil(perPage/numOfSrcs) * numOfSrcs
	}

	//// Get equal number of entries for each item
	// Set limit to the smallest of qLimit and perPage (adapts to each page)
	limit := perPage
	if limitIsSet && qLimit-page*perPage < perPage {
		limit = qLimit - page*perPage
		if limit < 0 { // blank page
			limit = 0
		}
	}
	perItem := math.Ceil(limit / numOfSrcs)
	perItems := make([]int, _numOfSrcs)
	for i := range perItems {
		perItems[i] = int(perItem)
	}
	perItems[_numOfSrcs-1] += int(limit - numOfSrcs*perItem) // add padding to the last item

	//// Calculate offset for items
	// Set limit to the smallest of qLimit and perPage (regardless of current page number)
	Limit := perPage
	if limitIsSet && qLimit < perPage {
		Limit = qLimit
	}
	offset := page * math.Ceil(Limit/numOfSrcs)
	offsets := make([]int, _numOfSrcs)
	for i := range offsets {
		offsets[i] = int(offset)
	}
	offsets[_numOfSrcs-1] += int(page * (limit - numOfSrcs*perItem)) // add padding to the last offset

	return perItems, offsets
}
