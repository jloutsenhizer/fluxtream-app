package com.fluxtream.services.impl;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TimeZone;
import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import com.fluxtream.Configuration;
import com.fluxtream.connectors.Connector;
import com.fluxtream.connectors.google_latitude.LocationFacet;
import com.fluxtream.connectors.vos.AbstractFacetVO;
import com.fluxtream.domain.AbstractFloatingTimeZoneFacet;
import com.fluxtream.domain.metadata.City;
import com.fluxtream.domain.metadata.DayMetadataFacet;
import com.fluxtream.domain.metadata.DayMetadataFacet.TravelType;
import com.fluxtream.domain.metadata.DayMetadataFacet.VisitedCity;
import com.fluxtream.domain.metadata.WeatherInfo;
import com.fluxtream.services.GuestService;
import com.fluxtream.services.JPADaoService;
import com.fluxtream.services.MetadataService;
import com.fluxtream.services.NotificationsService;
import com.fluxtream.thirdparty.helpers.WWOHelper;
import com.fluxtream.utils.JPAUtils;
import com.fluxtream.utils.Utils;
import org.apache.commons.httpclient.HttpException;
import org.apache.log4j.Logger;
import org.joda.time.MutableDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Component
@Transactional(readOnly=true)
public class MetadataServiceImpl implements MetadataService {

	Logger logger = Logger.getLogger(MetadataServiceImpl.class);

	@Autowired
	Configuration env;

	@PersistenceContext
	EntityManager em;

	@Autowired
	GuestService guestService;

	@Autowired
	NotificationsService notificationsService;

    @Autowired
    WWOHelper wwoHelper;

    @Autowired
    ServicesHelper servicesHelper;

    private static final DateTimeFormatter formatter = DateTimeFormat
            .forPattern("yyyy-MM-dd");

    @Override
	public TimeZone getTimeZone(double latitude, double longitude) {
		City closestCity = getClosestCity(latitude, longitude);
		TimeZone timeZone = TimeZone.getTimeZone(closestCity.geo_timezone);
		return timeZone;
	}

	@Override
	public TimeZone getCurrentTimeZone(long guestId) {
		LocationFacet lastLocation = getLastLocation(guestId,
                                                     System.currentTimeMillis());
		if (lastLocation != null) {
			TimeZone timeZone = getTimeZone(lastLocation.latitude,
					lastLocation.longitude);
			return timeZone;
		}
		return null;
	}

	@Override
	@Transactional(readOnly = false)
	public void setTimeZone(long guestId, String date, String timeZone) {
		DayMetadataFacet context = getDayMetadata(guestId, date, true);
		servicesHelper.setTimeZone(context, timeZone);
		em.merge(context);
	}

	@Override
	public void setTraveling(long guestId, String date, TravelType travelType) {
		DayMetadataFacet info = getDayMetadata(guestId, date, false);
		if (info == null)
			return;
		info.travelType = travelType;
		em.merge(info);
	}

	@Override
	public void addTimeSpentAtHome(long guestId, long startTime, long endTime) {
		// TODO Auto-generated method stub

	}

	@Override
	@Transactional(readOnly = false)
	public DayMetadataFacet getDayMetadata(long guestId, String date,
			boolean create) {
		DayMetadataFacet info = JPAUtils.findUnique(em, DayMetadataFacet.class,
                                                    "context.byDate", guestId, date);
		if (info != null)
			return info;
		else if (create)
			info = copyNextDailyContextualInfo(guestId, date);
		if (info != null)
			return info;
		else {
			info = new DayMetadataFacet();
			info.date = date;
			info.guestId = guestId;
			return info;
		}
	}

    @Override
    public List<DayMetadataFacet> getAllDayMetadata(final long guestId) {
        return JPAUtils.find(em, DayMetadataFacet.class,"context.all",guestId);
    }

    @Transactional(readOnly = false)
	public DayMetadataFacet copyNextDailyContextualInfo(long guestId,
			String date) {
		DayMetadataFacet next = getNextExistingDayMetadata(guestId, date);
		DayMetadataFacet info = new DayMetadataFacet();
		info.guestId = guestId;
		info.date = date;

		if (next == null) {
			return null;
		} else {
			City mainCity = getMainCity(guestId, next);
			if (mainCity != null) {
				servicesHelper.addCity(info, mainCity);
				servicesHelper.setTimeZone(info, next.timeZone);
				TimeZone tz = TimeZone.getTimeZone(next.timeZone);
				List<WeatherInfo> weatherInfo = getWeatherInfo(
						mainCity.geo_latitude, mainCity.geo_longitude, date,
                        AbstractFacetVO.toMinuteOfDay(new Date(info.start), tz),
                        AbstractFacetVO.toMinuteOfDay(new Date(info.end), tz));
				setWeatherInfo(info, weatherInfo);
			}
			servicesHelper.setTimeZone(info, next.timeZone);
		}
		em.persist(info);
		return info;
	}

	private DayMetadataFacet getNextExistingDayMetadata(long guestId,
			String todaysDate) {
		// TODO: not totally acurate, since we are ignoring the timezone
		// of todaysDate, but should work in most cases
		long start = formatter.parseMillis(todaysDate);
		DayMetadataFacet next = JPAUtils.findUnique(em, DayMetadataFacet.class,
				"context.day.next", guestId, start);
		if (next == null)
			next = JPAUtils.findUnique(em, DayMetadataFacet.class,
					"context.day.oldest", guestId);
		return next;
	}

	@Override
	public City getMainCity(long guestId, DayMetadataFacet context) {
		NavigableSet<VisitedCity> cities = context.getOrderedCities();
		if (cities.size() == 0) {
			logger.warn("guestId=" + guestId + " message=no_main_city date=" + context.date);
			return null;
		}
		VisitedCity mostVisited = cities.last();
        City city = null;
        if (mostVisited.state!=null)
            city = JPAUtils.findUnique(em, City.class,
                                       "city.byNameStateAndCountryCode", mostVisited.name,
                                       mostVisited.state,
                                       env.getCountryCode(mostVisited.country));
        else
            city = JPAUtils.findUnique(em, City.class,
                    "city.byNameAndCountryCode", mostVisited.name,
                    env.getCountryCode(mostVisited.country));
		return city;
	}

	private void setWeatherInfo(DayMetadataFacet info,
			List<WeatherInfo> weatherInfo) {
		if (weatherInfo.size() == 0)
			return;

		for (WeatherInfo weather : weatherInfo) {
			if (weather.tempC < info.minTempC)
				info.minTempC = weather.tempC;
			if (weather.tempF < info.minTempF)
				info.minTempF = weather.tempF;
			if (weather.tempC > info.maxTempC)
				info.maxTempC = weather.tempC;
			if (weather.tempF > info.maxTempF)
				info.maxTempF = weather.tempF;
		}

	}

	@Override
	public LocationFacet getLastLocation(long guestId, long time) {
		LocationFacet lastSeen = JPAUtils.findUnique(em, LocationFacet.class,
                                                     "google_latitude.location.lastSeen", guestId, time);
		return lastSeen;
	}

	@Override
	public LocationFacet getNextLocation(long guestId, long time) {
		LocationFacet lastSeen = JPAUtils.findUnique(em, LocationFacet.class,
				"google_latitude.location.nextSeen", guestId, time);
		return lastSeen;
	}

	@Override
	public DayMetadataFacet getLastDayMetadata(long guestId) {
		DayMetadataFacet lastDay = JPAUtils.findUnique(em,
				DayMetadataFacet.class, "context.day.last", guestId);
		return lastDay;
	}

	@Override
	public TimeZone getTimeZone(long guestId, String date) {
		DayMetadataFacet thatDay = JPAUtils.findUnique(em,
				DayMetadataFacet.class, "context.byDate", guestId, date);
		if (thatDay != null)
			return TimeZone.getTimeZone(thatDay.timeZone);
		else {
			logger.warn("guestId=" + guestId + " action=getTimeZone warning=returning UTC Timezone");
			return TimeZone.getTimeZone("UTC"); // Code should never go here!
		}
	}

	@Override
	public TimeZone getTimeZone(long guestId, long time) {
		DayMetadataFacet thatDay = JPAUtils.findUnique(em,
				DayMetadataFacet.class, "context.day.when", guestId, time, time);
		if (thatDay != null)
			return TimeZone.getTimeZone(thatDay.timeZone);
		else {
			logger.warn("guestId=" + guestId + " action=getTimeZone warning=returning UTC Timezone");
			return TimeZone.getTimeZone("UTC"); // Code should never go here!
		}
	}

	@Override
	@Transactional(readOnly=false)
	public void setDayCommentTitle(long guestId, String date, String title) {
		DayMetadataFacet thatDay = getDayMetadata(guestId,
				date, true);
		thatDay.title = title;
		em.merge(thatDay);
	}

	@Override
	@Transactional(readOnly=false)
	public void setDayCommentBody(long guestId, String date, String body) {
		DayMetadataFacet thatDay = JPAUtils.findUnique(em,
				DayMetadataFacet.class, "context.byDate", guestId, date);
		thatDay.comment = body;
		em.merge(thatDay);
	}

    @Override
    public City getClosestCity(double latitude, double longitude) {

        List<City> cities = new ArrayList<City>();
        for (int dist = 10, i = 1; cities.size() == 0;)
            cities = getClosestCities(latitude, longitude,
                                      Double.valueOf(dist ^ i++));

        return cities.get(0);
    }

    @Override
    public List<City> getClosestCities(double latitude, double longitude,
                                       double dist) {

        double lon1 = longitude - dist
                                  / Math.abs(Math.cos(Math.toRadians(latitude)) * 69d);
        double lon2 = longitude + dist
                                  / Math.abs(Math.cos(Math.toRadians(latitude)) * 69d);
        double lat1 = latitude - (dist / 69.d);
        double lat2 = latitude + (dist / 69.d);

        Query query = em
                .createNativeQuery(
                        "SELECT cities1000.geo_id, cities1000.geo_name, "
                        + "cities1000.geo_timezone, cities1000.geo_latitude, "
                        + "cities1000.geo_longitude, cities1000.geo_country_code, "
                        + "cities1000.geo_admin1_code, cities1000.population, "
                        + "3956 * 2 * ASIN(SQRT(POWER(SIN((:mylat - geo_latitude) * pi()/180 / 2), 2) + COS(:mylat * pi()/180) *COS(geo_latitude * pi()/180) * POWER(SIN((:mylon -geo_longitude) * pi()/180 / 2), 2))) as distance "
                        + "FROM cities1000 "
                        + "WHERE geo_longitude between :lon1 and :lon2 "
                        + "and geo_latitude between :lat1 and :lat2 "
                        + "HAVING distance < :dist ORDER BY distance limit 1;",
                        City.class);

        query.setParameter("mylat", latitude);
        query.setParameter("mylon", longitude);
        query.setParameter("lon1", lon1);
        query.setParameter("lon2", lon2);
        query.setParameter("lat1", lat1);
        query.setParameter("lat2", lat2);
        query.setParameter("dist", dist);

        @SuppressWarnings("unchecked")
        List<City> resultList = query.getResultList();
        return resultList;
    }

    @Override
    public List<WeatherInfo> getWeatherInfo(double latitude, double longitude,
                                            String date, int startMinute, int endMinute) {
        City closestCity = getClosestCity(latitude, longitude);
        List<WeatherInfo> weather = JPAUtils.find(em, WeatherInfo.class,
                                                  "weather.byDateAndCity.between", closestCity.geo_name, date,
                                                  startMinute, endMinute);

        if (weather != null && weather.size() > 0) {
            addIcons(weather);
            return weather;
        }
        else {
            try {
                fetchWeatherInfo(latitude, longitude, closestCity.geo_name,
                                 date);
            } catch (Exception e) {
                logger.warn("action=fetchWeather error");
            }
            weather = JPAUtils.find(em, WeatherInfo.class,
                                    "weather.byDateAndCity.between", closestCity.geo_name,
                                    date, startMinute, endMinute);
            addIcons(weather);
        }
        return weather;
    }

    @Override
    /**
     * updateGeolocationInfo notifies the system that timezone information may have changed on certain
     * days.  This allows a batch upload process to make several updates and then do a single overall
     * timezone update, since applying the updates incrementally might be more expensive.
     *
     *
     * contextual info table maps date to optional timezone name
     * - if more than one data point with timezone was recorded for that day, use the most frequently reported
     *   timezone (the "mode")
     *
     * Proposed algorithm:
     * Input:
     * Set<String> updatedDates: dates which we have added new timezone information for, and whose consensus timezone
     *                           might now need to be changed
     *
     * Local:
     * Set<String> needToUpdateDates
     *
     * - For each date in dates:
     *      Calculate timezone from this date by finding the mode of all timezone measurements for this date, and
     *      compare to what's already in the contextual info table.  If it's the same, continue to the next date.
     *
     *      If new timezone doesn't match, write the next value into the contextual info table, and then find other
     *      dates which might be affected:
     *
     *      Search backwards in time through the contextual info table until an earlier date is found that already
     *      has timezone set ("lastKnownTimezoneDate").  Any dates after "lastKnownTimezoneDate" and up to "date" are
     *      also affected by the timezone change in date; add these to needToUpdateDates.  Also add two days before this
     *      range (assing "lastKnownTimezoneDate" and the day before it) and one day after (adding the day after "date").
     *      This guarantees (a) that we deal correctly with facets whose data started one day before the recorded
     *      date (e.g. Zeo, which is recorded on the wakeup date rather than the go to sleep date), and (b) that
     *      any overlap due to moving samples forwards or backwards in time due to changing timezones is handled
     *      correctly.
     *
     * - Convert neetToUpdateDates into one or more ranges of contiguous dates.  For example, Oct/1 Oct/2 Oct/3 Oct/6 Oct/7 Oct/9
     *   will be converted to [Oct/1 - Oct/3] [Oct/6 - Oct/7] [Oct/9 - Oct/9]
     *
     * - For each range:
     *      - For each connector that has floating facets:
     *          - For facets from this connector that have a date included in this date range
     *              - Recompute start and end times for this facet given the new time zone\
     *          - Compute datastoreTimespan as Java times, from beginning (00:00) of start date to the end of end date (23:59:59.99999),
     *            using timezones from start and end.  (Note that these timezones have not changed, since we added extra
     *            unchanged dates on either side of the range in the earlier step)
     *          - Find all facets from this connector whose start and end times overlap with datastoreTimespan.  Note that this
     *            can include facets whose "date" isn't inside the date range, since some of the start/end times span a day before
     *            the facet's "date".
     *          - Ask datastore to erase data from datastoreTimespan, and to load all facets found from previous step.
     *            This should be done atomically with a single call to "import"
     */
    @Transactional(readOnly=false)
    public void updateGeolocationInfo(final long guestId, final Set<String> updatedDates) {
        List<String> needToUpdateDates = new ArrayList<String>();
        // this map will contain the resulting date-to-timezone mapping
        Map<String,String> timezoneDates = new HashMap<String,String>();
        for (String updatedDate : updatedDates) {
            // Calculate timezone from this date by finding the mode of all timezone measurements for this date,
            String timezone = findTimezoneMode(guestId, updatedDate);
            DayMetadataFacet dayMetadata = getDayMetadata(guestId, updatedDate, false);
            // compare to what's already in the contextual info table.
            if (TimeZone.getTimeZone(dayMetadata.timeZone).equals(TimeZone.getTimeZone(timezone)))
                //  If it's the same, continue to the next date.
                continue;
            else {
                // If new timezone doesn't match, write the next value into the contextual info table, and then find other
                // dates which might be affected:
                dayMetadata.timeZone = timezone;
                em.persist(dayMetadata);
                // Search backwards in time through the contextual info table until an earlier date is found that already
                // has timezone set ("lastKnownTimezoneDate").  Any dates after "lastKnownTimezoneDate" and up to "date" are
                //  also affected by the timezone change in date; add these to needToUpdateDates.
                // Also add two days before this
                // range (assing "lastKnownTimezoneDate" and the day before it) and one day after (adding the day after "date").
                // This guarantees (a) that we deal correctly with facets whose data started one day before the recorded
                // date (e.g. Zeo, which is recorded on the wakeup date rather than the go to sleep date), and (b) that
                // any overlap due to moving samples forwards or backwards in time due to changing timezones is handled
                // correctly.
                String lastKnownTimezoneDate = getLastKnownTimezoneDate(guestId, dayMetadata.start);
                if (lastKnownTimezoneDate!=null) {
                    List<String> daysBetween = daysBetween(lastKnownTimezoneDate, updatedDate);
                    for (String dayBetween : daysBetween) {
                        needToUpdateDates.add(dayBetween);
                        timezoneDates.put(dayBetween, timezone);
                    }
                    final List<String> daysBefore = daysBefore(lastKnownTimezoneDate, 2);
                    for (String dayBefore : daysBefore)
                        timezoneDates.put(dayBefore, timezone);
                    needToUpdateDates.addAll(daysBefore);
                }
                else {
                    final List<String> daysBefore = daysBefore(updatedDate, 2);
                    needToUpdateDates.addAll(daysBefore);
                    for (String dayBefore : daysBefore)
                        timezoneDates.put(dayBefore, timezone);
                }
                needToUpdateDates.add(updatedDate);
                timezoneDates.put(updatedDate, timezone);
                // also adding one day of padding after -- ANNE: do you agree?
                final List<String> daysAfter = daysAfter(updatedDate, 1);
                needToUpdateDates.addAll(daysAfter);
                timezoneDates.put(daysAfter.get(0), timezone);
            }
        }

        final List<List<String>> dateRanges = getDateRanges(needToUpdateDates);

        // For each range:
        for (List<String> dateRange : dateRanges) {
            // For each connector that has floating facets:
            final List<Class<? extends AbstractFloatingTimeZoneFacet>> floatingTimeZoneFacetClasses = Connector.getFloatingTimeZoneFacetClasses();
            for (Class<? extends AbstractFloatingTimeZoneFacet> floatingTimeZoneFacetClass : floatingTimeZoneFacetClasses) {
                // For facets from this connector that have a date included in this date range
                List<AbstractFloatingTimeZoneFacet> facetsToFix = findFacetsInRange(guestId, dateRange, floatingTimeZoneFacetClass);
                for (AbstractFloatingTimeZoneFacet floatingTimeZoneFacet : facetsToFix) {
                    String timezone = timezoneDates.get(floatingTimeZoneFacet.date);
                    try {
                        // Recompute start and end times for this facet given the new time zone
                        floatingTimeZoneFacet.updateTimeInfo(TimeZone.getTimeZone(timezone));
                    } catch (ParseException e) {
                        StringBuilder sb = new StringBuilder("module=updateQueue component=metadataServiceImpl action=updateGeolocationInfo")
                                .append(" facetClass=").append(floatingTimeZoneFacetClass.getName())
                                .append(" guestId=").append(guestId)
                                .append(" message=\"Couldn't update geolocationInfo (parse exception)\"")
                                .append(" stackTrace=<![CDATA[").append(Utils.stackTrace(e)).append("]]>");
                        logger.warn(sb.toString());
                    }
                }
            }
            // Compute datastoreTimespan as Java times, from beginning (00:00) of start date to the end of end date (23:59:59.99999),
            // using timezones from start and end.  (Note that these timezones have not changed, since we added extra
            // unchanged dates on either side of the range in the earlier step)
            // Find all facets from this connector whose start and end times overlap with datastoreTimespan.  Note that this
            // can include facets whose "date" isn't inside the date range, since some of the start/end times span a day before
            // the facet's "date".
            // Ask datastore to erase data from datastoreTimespan, and to load all facets found from previous step.  This should be done
            // atomically with a single call to "import"
        }

    }

    private List<AbstractFloatingTimeZoneFacet> findFacetsInRange(final long guestId, final List<String> dateRange, final Class<? extends AbstractFloatingTimeZoneFacet> floatingTimeZoneFacetClass) {
        Entity entityAnnotation = floatingTimeZoneFacetClass.getAnnotation(Entity.class);
        String entityName = entityAnnotation.name();
        String queryString = new StringBuilder("SELECT facet FROM ").append(entityName).append(" WHERE facet.guestId=? AND facet.date IN ?").toString();
        final Query query = em.createQuery(queryString);
        query.setParameter(1, guestId);
        query.setParameter(2, dateRange);
        return query.getResultList();
    }

    /**
     * Convert dates into one or more ranges of contiguous dates.  For example, Oct/1 Oct/2 Oct/3 Oct/6 Oct/7 Oct/9
     * will be converted to [Oct/1 - Oct/3] [Oct/6 - Oct/7] [Oct/9 - Oct/9]
     * This method assumes dates are sorted in growing order
     * @param dates
     * @return
     */
    List<List<String>> getDateRanges(final List<String> dates) {
        final List<List<String>> dateRanges = new ArrayList<List<String>>();
        if (dates.size()==0) return dateRanges;
        final Iterator<String> eachDate = dates.iterator();
        final String firstDate = eachDate.next();
        MutableDateTime startDateTime = formatter.parseMutableDateTime(firstDate);
        final MutableDateTime endDateTime = formatter.parseMutableDateTime(dates.get(dates.size() - 1));
        List<String> dateRange = addDateRangeWithDate(dateRanges, firstDate);
        while(startDateTime.isBefore(endDateTime)&&eachDate.hasNext()) {
            startDateTime.addDays(1);
            final String nextDate = eachDate.next();
            final String nextDateInRange = formatter.print(startDateTime);
            if (nextDateInRange.equals(nextDate))
                dateRange.add(nextDate);
            else {
                dateRange = addDateRangeWithDate(dateRanges, nextDate);
                startDateTime = formatter.parseMutableDateTime(nextDate);
            }
        }
        return dateRanges;
    }

    private List<String> addDateRangeWithDate(final List<List<String>> dateRanges, final String date) {
        List<String> dateRange = new ArrayList<String>();
        dateRange.add(date);
        dateRanges.add(dateRange);
        return dateRange;
    }

    List<String> daysBetween(final String startDate, final String endDate) {
        List<String> daysBetween = new ArrayList<String>();
        final MutableDateTime startDateTime = formatter.parseMutableDateTime(startDate);
        final MutableDateTime endDateTime = formatter.parseMutableDateTime(startDate);
        if (startDateTime.isAfter(endDateTime.toInstant()))
            return daysBetween;
        while(true) {
            startDateTime.addDays(1);
            final String dayBetween = formatter.print(startDateTime);
            if (!dayBetween.equals(endDate))
                daysBetween.add(dayBetween);
            else break;
        }
        return daysBetween;
    }

    private String getLastKnownTimezoneDate(final long guestId, long ts) {
        final DayMetadataFacet dayMetadataFacet = JPAUtils.findUnique(em, DayMetadataFacet.class, "context.day.before", guestId, ts);
        if (dayMetadataFacet!=null)
            return dayMetadataFacet.date;
        else return null;
    }

    List<String> daysAfter(final String date, final int days) {
        final MutableDateTime startDateTime = formatter.parseMutableDateTime(date);
        List<String> daysAfter = new ArrayList<String>();
        for (int i=0; i<days; i++) {
            startDateTime.addDays(1);
            final String dayAfter = formatter.print(startDateTime);
            daysAfter.add(dayAfter);
        }
        return daysAfter;
    }

    List<String> daysBefore(final String date, final int days) {
        final MutableDateTime startDateTime = formatter.parseMutableDateTime(date);
        List<String> daysBefore = new ArrayList<String>();
        for (int i=0; i<days; i++) {
            startDateTime.addDays(-1);
            final String dayAfter = formatter.print(startDateTime);
            daysBefore.add(dayAfter);
        }
        Collections.reverse(daysBefore);
        return daysBefore;
    }

    private String findTimezoneMode(final long guestId, final String date) {
        final List<LocationFacet> facets = JPAUtils.find(em, LocationFacet.class, "google_latitude.location.byDate", guestId, date);
        Map<String, Integer> timezoneCounts = new HashMap<String,Integer>();
        for (LocationFacet locationFacet : facets) {
            if (locationFacet.timezone!=null) {
                final Integer previousCount = timezoneCounts.get(locationFacet.timezone);
                int count = previousCount==null?1:previousCount+1;
                timezoneCounts.put(locationFacet.timezone, count);
            }
        }
        int max = Collections.max(timezoneCounts.values());
        for (String tz : timezoneCounts.keySet()) {
            if (timezoneCounts.get(tz)==max)
                return tz;
        }
        throw new RuntimeException("Couldn't find timezone for date " + date + ", guestId=guestId");
    }

    @Transactional(readOnly = false)
    private void fetchWeatherInfo(double latitude, double longitude,
                                  String city, String date) throws HttpException, IOException {
        List<WeatherInfo> weatherInfo = wwoHelper.getWeatherInfo(latitude,
                                                                 longitude, date);
        for (WeatherInfo info : weatherInfo) {
            info.city = city;
            info.fdate = date;
            em.persist(info);
        }
    }

    private void addIcons(List<WeatherInfo> weather){
        for (WeatherInfo weatherInfo : weather){
            switch (weatherInfo.weatherCode){
                case 395://Moderate or heavy snow in area with thunder
                    weatherInfo.weatherIconUrl = "images/climacons/CS.png";
                    weatherInfo.weatherIconUrlDay = "images/climacons/CSS.png";
                    weatherInfo.weatherIconUrlNight = "images/climacons/CSM.png";
                    break;
                case 389://Moderate or heavy rain in area with thunder
                    weatherInfo.weatherIconUrl = "images/climacons/CL.png";
                    weatherInfo.weatherIconUrlDay = "images/climacons/CL.png";
                    weatherInfo.weatherIconUrlNight = "images/climacons/CL.png";
                    break;
                case 200://Thundery outbreaks in nearby
                case 386://Patchy light rain in area with thunder
                case 392://Patchy light snow in area with thunder
                    weatherInfo.weatherIconUrl = "images/climacons/CL.png";
                    weatherInfo.weatherIconUrlDay = "images/climacons/CLS.png";
                    weatherInfo.weatherIconUrlNight = "images/climacons/CLM.png";
                    break;
                case 113://Clear/Sunny
                    weatherInfo.weatherIconUrl = "images/climacons/Sun.png";
                    weatherInfo.weatherIconUrlDay = "images/climacons/Sun.png";
                    weatherInfo.weatherIconUrlNight = "images/climacons/Moon.png";
                    break;
                case 116://Partly Cloudy
                    weatherInfo.weatherIconUrl = "images/climacons/Cloud.png";
                    weatherInfo.weatherIconUrlDay = "images/climacons/CS1.png";//CS#1.png
                    weatherInfo.weatherIconUrlNight = "images/climacons/CM.png";
                    break;
                case 122://Overcast
                case 119://Cloudy
                    weatherInfo.weatherIconUrl = "images/climacons/Cloud.png";
                    weatherInfo.weatherIconUrlDay = "images/climacons/Cloud.png";
                    weatherInfo.weatherIconUrlNight = "images/climacons/Cloud.png";
                    break;
                case 299://Moderate rain at times
                case 302://Moderate rain
                case 305://Heavy rain at times
                case 308://Heavy rain
                case 296: //Light rain
                case 293: //Patchy light rain
                case 266://Light drizzle
                case 353://Light rain shower
                case 356://Moderate or heavy rain shower
                case 359://Torrentail rain shower
                    weatherInfo.weatherIconUrl = "images/climacons/CD.png";
                    weatherInfo.weatherIconUrlDay = "images/climacons/CDS.png";
                    weatherInfo.weatherIconUrlNight = "images/climacons/CDM.png";
                    break;
                case 263://patchy light drizzle
                case 176://patchy rain nearby
                case 143://Mist
                    weatherInfo.weatherIconUrl = "images/climacons/CD_Alt.png";
                    weatherInfo.weatherIconUrlDay = "images/climacons/CDS_Alt.png";
                    weatherInfo.weatherIconUrlNight = "images/climacons/CDM_Alt.png";
                    break;
                case 227://Blowing snow
                case 230://Blizzard
                case 329://Patchy moderate snow
                case 332://Moderate snow
                case 335://Patchy heavy snow
                case 338://Heavy snow
                case 368://Light snow showers
                case 371://Moderate or heavy snow showers
                    weatherInfo.weatherIconUrl = "images/climacons/CS.png";
                    weatherInfo.weatherIconUrlDay = "images/climacons/CSS.png";
                    weatherInfo.weatherIconUrlNight = "images/climacons/CSM.png";
                    break;
                case 179://Patchy snow nearby
                case 323://Patchy Light snow
                case 325://Light snow
                    weatherInfo.weatherIconUrl = "images/climacons/CSA.png";
                    weatherInfo.weatherIconUrlDay = "images/climacons/CSSA.png";
                    weatherInfo.weatherIconUrlNight = "images/climacons/CSMA.png";
                    break;
                case 281://Freezing drizzle
                case 185: //Patchy freezing drizzle nearby
                case 182://Patchy sleet nearby
                case 311://Light freezing rain
                case 317://Light sleet
                    weatherInfo.weatherIconUrl = "images/climacons/CH.png";
                    weatherInfo.weatherIconUrlDay = "images/climacons/CHS.png";
                    weatherInfo.weatherIconUrlNight = "images/climacons/CHM.png";
                    break;
                case 260://Freezing Fog
                case 248://Fog
                    weatherInfo.weatherIconUrl = "images/climacons/Fog.png";
                    weatherInfo.weatherIconUrlDay = "images/climacons/FS.png";
                    weatherInfo.weatherIconUrlNight = "images/climacons/FM.png";
                    break;
                case 314://Moderate or Heavy freezing rain
                case 320://Moderate or heavy sleet
                case 284://Heavy freezing drizzle
                case 350://Ice pellets
                case 362://Light sleet showers
                case 365://Moderate or heavy sleet
                case 374://Light showrs of ice pellets
                case 377://Moderate or heavy showres of ice pellets
                    weatherInfo.weatherIconUrl = "images/climacons/CH_Alt.png";
                    weatherInfo.weatherIconUrlDay = "images/climacons/CHS_Alt.png";
                    weatherInfo.weatherIconUrlNight = "images/climacons/CHM_Alt.png";
                    break;
                default:
                    weatherInfo.weatherIconUrl = "images/climacons/WC.png";
                    weatherInfo.weatherIconUrlDay = "images/climacons/WC.png";
                    weatherInfo.weatherIconUrlNight = "images/climacons/WC.png";
                    break;
            }
        }
    }

}
