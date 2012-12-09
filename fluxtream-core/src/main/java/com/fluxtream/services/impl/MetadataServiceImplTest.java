package com.fluxtream.services.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.collections.iterators.ListIteratorWrapper;
import org.junit.Assert;

/**
 *
 * @author Candide Kemmler (candide@fluxtream.com)
 */
public class MetadataServiceImplTest {

    MetadataServiceImpl metadataService = new MetadataServiceImpl();

    @org.junit.Test
    public void testGetDateRanges() throws Exception {
        //Oct/1 Oct/2 Oct/3 Oct/6 Oct/7 Oct/9
        final List<String> dates = Arrays.asList("2012-10-01", "2012-10-02", "2012-10-03", "2012-10-06", "2012-10-07", "2012-10-09");
        final List<List<String>> dateRanges = metadataService.getDateRanges(dates);
        Assert.assertTrue(dateRanges.size()==3);
        Assert.assertTrue(dateRanges.get(0).size() == 3);
        Assert.assertTrue(dateRanges.get(1).size() == 2);
        Assert.assertTrue(dateRanges.get(2).size() == 1);
        Assert.assertTrue(dateRanges.get(2).get(0).equals("2012-10-09"));
    }

    @org.junit.Test
    public void testDaysBetween() throws Exception {
        final List<String> list = metadataService.daysBetween("2012-11-04", "2012-11-07");
        Assert.assertTrue(list.size()==2);
        Assert.assertTrue(list.contains("2012-11-05"));
        Assert.assertTrue(list.contains("2012-11-06"));
    }

    @org.junit.Test
    public void testDaysAfter() throws Exception {
        final List<String> list = metadataService.daysAfter("2012-11-07", 1);
        Assert.assertTrue(list.size()==1);
        Assert.assertTrue(list.contains("2012-11-08"));
    }

    @org.junit.Test
    public void testDaysBefore() throws Exception {
        final List<String> list = metadataService.daysBefore("2012-11-07", 2);
        Assert.assertTrue(list.size()==2);
        Assert.assertTrue(list.get(0).equals("2012-11-05"));
        Assert.assertTrue(list.get(1).equals("2012-11-06"));
    }
}
