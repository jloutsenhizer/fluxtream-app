package com.fluxtream.connectors.google_latitude;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import com.fluxtream.connectors.Connector.UpdateStrategyType;
import com.fluxtream.connectors.annotations.JsonFacetCollection;
import com.fluxtream.connectors.annotations.Updater;
import com.fluxtream.connectors.controllers.GoogleOAuth2Helper;
import com.fluxtream.connectors.updaters.AbstractGoogleOAuthUpdater;
import com.fluxtream.connectors.updaters.UpdateInfo;
import com.fluxtream.domain.ApiUpdate;
import com.fluxtream.domain.metadata.City;
import com.fluxtream.services.ApiDataService;
import com.fluxtream.services.GuestService;
import com.fluxtream.services.MetadataService;
import com.fluxtream.utils.Utils;
import com.google.api.client.googleapis.json.JsonCParser;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Updater(prettyName = "Latitude", value = 2, objectTypes = { LocationFacet.class }, updateStrategyType = UpdateStrategyType.INCREMENTAL)
@JsonFacetCollection(LocationFacetVOCollection.class)
public class GoogleLatitudeUpdater extends AbstractGoogleOAuthUpdater {

	@Autowired
	GuestService guestService;

	@Autowired
	ApiDataService apiDataService;

    @Autowired
    GoogleOAuth2Helper oAuth2Helper;

    @Autowired
    MetadataService metadataService;

	public GoogleLatitudeUpdater() {
		super();
	}

    private static final DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd");

	@Override
	public void updateConnectorDataHistory(UpdateInfo updateInfo)
			throws Exception {
        // accumulate dates affected by this import
        Set<String> updatedDates = new HashSet<String>();
		loadHistory(updateInfo, 0, System.currentTimeMillis(), updatedDates);
        // call metadataService method to update geolocation info for these dates
        metadataService.updateGeolocationInfo(updateInfo.getGuestId(), updatedDates);
	}

	public void updateConnectorData(UpdateInfo updateInfo) throws Exception {
		ApiUpdate lastSuccessfulUpdate = connectorUpdateService
				.getLastSuccessfulUpdate(updateInfo.apiKey.getGuestId(),
						connector());
        // accumulate dates affected by this import
        Set<String> updatedDates = new HashSet<String>();
		loadHistory(updateInfo, lastSuccessfulUpdate.ts,
				System.currentTimeMillis(), updatedDates);
        // call metadataService method to update geolocation info for these dates
        metadataService.updateGeolocationInfo(updateInfo.getGuestId(), updatedDates);
	}

	private void loadHistory(UpdateInfo updateInfo, long from, long to, Set<String> updatedDates)
			throws Exception {
        String accessToken = oAuth2Helper.getAccessToken(updateInfo.getGuestId(), updateInfo.apiKey.getConnector());
		HttpTransport transport = this.getTransport(updateInfo.apiKey);
		String key = env.get("google_latitudeApiKey");
		List<LocationFacet> locationList = executeList(updateInfo, transport,
				key, 1000, from, to, accessToken);
		if (locationList != null && locationList.size() > 0) {
			List<LocationFacet> storedLocations = new ArrayList<LocationFacet>();
			for (LocationFacet locationResource : locationList) {
				if (locationResource.timestampMs==0)
					continue;

                final City closestCity =
                        metadataService.getClosestCity(locationResource.latitude, locationResource.longitude);
                locationResource.timezone = closestCity.geo_timezone;
                TimeZone timeZone = TimeZone.getTimeZone(locationResource.timezone);
                locationResource.date = format.withZone(DateTimeZone.forTimeZone(timeZone)).print(locationResource.timestampMs);

                locationResource.start = locationResource.timestampMs;
				locationResource.end = locationResource.timestampMs;

                apiDataService.addGuestLocation(updateInfo.getGuestId(),
						locationResource,
                        LocationFacet.Source.GOOGLE_LATITUDE);
				
				storedLocations.add(locationResource);
			}
			Collections.sort(storedLocations);
			LocationFacet oldest = storedLocations.get(0);
            loadHistory(updateInfo, from, oldest.timestampMs-1000, updatedDates);
		}
	}

	private List<LocationFacet> executeList(UpdateInfo updateInfo,
			HttpTransport transport, String key, int maxResults, long minTime,
			long maxTime, String accessToken) throws Exception {
		long then = System.currentTimeMillis();
		String requestUrl = "request url not set yet";
		try {
			transport.addParser(new JsonCParser());
			HttpRequest request = transport.buildGetRequest();
			LatitudeUrl latitudeUrl = LatitudeUrl.forLocation();
			latitudeUrl.maxResults = String.valueOf(maxResults);
			latitudeUrl.granularity = "best";
			latitudeUrl.minTime = String.valueOf(minTime);
			latitudeUrl.maxTime = String.valueOf(maxTime);
			latitudeUrl.put("location", "all");
            latitudeUrl.put("key", key);
            latitudeUrl.put("access_token", accessToken);
			request.url = latitudeUrl;
			requestUrl = latitudeUrl.build();
			HttpResponse response = request.execute();
			List<LocationFacet> result = response.parseAs(LocationList.class).items;
			countSuccessfulApiCall(updateInfo.apiKey.getGuestId(),
					updateInfo.objectTypes, then, requestUrl);
			return result;
		} catch (Exception e) {
			countFailedApiCall(updateInfo.apiKey.getGuestId(),
					updateInfo.objectTypes, then, requestUrl, Utils.stackTrace(e));
			throw e;
		}
	}

}
