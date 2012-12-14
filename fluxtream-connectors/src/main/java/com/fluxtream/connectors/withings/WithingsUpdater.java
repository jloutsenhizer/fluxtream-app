package com.fluxtream.connectors.withings;

import com.fluxtream.connectors.SignpostOAuthHelper;
import com.fluxtream.connectors.annotations.JsonFacetCollection;
import com.fluxtream.connectors.annotations.Updater;
import com.fluxtream.connectors.updaters.AbstractUpdater;
import com.fluxtream.connectors.updaters.UpdateInfo;
import com.fluxtream.domain.ApiUpdate;
import com.fluxtream.utils.Utils;
import net.sf.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.fluxtream.utils.HttpUtils.fetch;

@Component
@Updater(prettyName = "Withings", value = 4, objectTypes = {
		WithingsBPMMeasureFacet.class, WithingsBodyScaleMeasureFacet.class },
        extractor = WithingsFacetExtractor.class,
        defaultChannels = {"Withings.weight", "Withings.systolic", "Withings.diastolic", "Withings.heartPulse"})
@JsonFacetCollection(WithingsFacetVOCollection.class)
public class WithingsUpdater extends AbstractUpdater {

    @Autowired
    SignpostOAuthHelper signpostHelper;

	public WithingsUpdater() {
		super();
	}

	@Override
	protected void updateConnectorDataHistory(UpdateInfo updateInfo) throws Exception {
		// get user info and find out first seen date
		long then = System.currentTimeMillis();
		String json;

        final String userid = updateInfo.apiKey.getAttributeValue("userid", env);
        String url = new StringBuilder("http://wbsapi.withings.net/measure?action=getmeas")
            .append("&userid=").append(userid)
            .append("&startdate=0")
            .append("&enddate=" + System.currentTimeMillis() / 1000).toString();

		try {
            json = signpostHelper.makeRestCall(connector(), updateInfo.apiKey, 3, url);
            JSONObject jsonObject = JSONObject.fromObject(json);
            if (jsonObject.getInt("status")!=0)
                throw new Exception("Unexpected status code " + jsonObject.getInt("status"));
            countSuccessfulApiCall(updateInfo.apiKey.getGuestId(),
                                   updateInfo.objectTypes, then, url);
		} catch (Exception e) {
			countFailedApiCall(updateInfo.apiKey.getGuestId(),
					updateInfo.objectTypes, then, url, Utils.stackTrace(e));
			throw e;
		}
		if (!json.equals(""))
			apiDataService.cacheApiDataJSON(updateInfo, json, -1, -1);
	}

	public void updateConnectorData(UpdateInfo updateInfo) throws Exception {
		long then = System.currentTimeMillis();
		String json;
		
		ApiUpdate lastSuccessfulUpdate = connectorUpdateService
				.getLastSuccessfulUpdate(updateInfo.apiKey.getGuestId(),
						connector());

		String url = new StringBuilder("http://wbsapi.withings.net/measure?action=getmeas")
                .append("&userid=" + updateInfo.apiKey.getAttributeValue("userid", env))
                .append("&startdate=" + lastSuccessfulUpdate.ts / 1000)
                .append("&enddate=" + System.currentTimeMillis() / 1000).toString();
		
		try {
            json = signpostHelper.makeRestCall(connector(), updateInfo.apiKey, 3, url);
            JSONObject jsonObject = JSONObject.fromObject(json);
            if (jsonObject.getInt("status")!=0)
                throw new Exception("Unexpected status code " + jsonObject.getInt("status"));
			countSuccessfulApiCall(updateInfo.apiKey.getGuestId(),
					updateInfo.objectTypes, then, url);
		} catch (Exception e) {
			countFailedApiCall(updateInfo.apiKey.getGuestId(),
					updateInfo.objectTypes, then, url, Utils.stackTrace(e));
			throw e;
		}
		apiDataService.cacheApiDataJSON(updateInfo, json, -1, -1);
	}

}
