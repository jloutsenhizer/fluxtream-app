package com.fluxtream.connectors.zeo;

import com.fluxtream.connectors.picasa.PicasaPhotoFacet;
import com.fluxtream.domain.AbstractFloatingTimeZoneFacet;
import com.fluxtream.utils.TestsUtils;
import org.junit.Test;

/**
 *
 * @author Candide Kemmler (candide@fluxtream.com)
 */
public class ZeoSleepStatsFacetTest {

    @Test
    public void testInheritanceTest() {
        System.out.println(AbstractFloatingTimeZoneFacet.class.isAssignableFrom(PicasaPhotoFacet.class));
    }

}
