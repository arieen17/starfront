var DaVinci;
if (typeof DaVinci === "undefined") {
  // Define DaVinci the first time the script is included
  DaVinci = {
    createDataLayer: function (vizType, vizURL, serverURL) {
      serverURL = serverURL || "";
      if (vizType === "Tile") {
        return new ol.layer.Tile({
          source: new ol.source.XYZ({
            url: serverURL + vizURL,
            attributions: 'Powered by <a href="https://davinci.cs.ucr.edu">&copy;DaVinci</a>'
          })
        });
      } else if (vizType === "Vector") {
        return new ol.layer.Vector({
             style: function() { return new ol.style.Style({
               image: pointDot,
               radius: 5,
               stroke: new ol.style.Stroke({color: "rgb(0,0,0)", width: 1}),
               fill: new ol.style.Fill({color: "rgba(0, 0, 0, 0.1)"})
             })},
             source: new ol.source.Vector({
               url: serverURL + vizURL,
               format: new ol.format.GeoJSON(),
               attributions: 'Powered by <a href="https://davinci.cs.ucr.edu">&copy;DaVinci</a>'
             })
         })
      }
    },
    addLoadEvent: function (func) {
      var oldonload = window.onload;
      if (typeof window.onload != 'function') {
        window.onload = func;
      } else {
        window.onload = function () {
          if (oldonload)
            oldonload();
          func();
        }
      }
    },

    loadScript: function (src) {
      var newScript = document.createElement('script');
      newScript.type = 'text/javascript';
      newScript.src = src;
      document.getElementsByTagName('head')[0].appendChild(newScript);
    },

    loadCSS: function (href) {
      var newStyle = document.createElement('link');
      newStyle.rel = 'stylesheet';
      newStyle.href = href;
      newStyle.type = 'text/css';
      document.getElementsByTagName('head')[0].appendChild(newStyle);
    }

  }
}

// Load OpenLayers if not loaded
if (typeof ol === 'undefined') {
  DaVinci.loadScript("https://cdn.rawgit.com/openlayers/openlayers.github.io/master/en/v5.3.0/build/ol.js");
  DaVinci.loadScript("https://cdn.polyfill.io/v2/polyfill.min.js?features=requestAnimationFrame,Element.prototype.classList,URL");
  DaVinci.loadCSS("https://cdn.rawgit.com/openlayers/openlayers.github.io/master/en/v5.3.0/css/ol.css");
  // TODO migrate to the latest version of OpenLayers
//  DaVinci.loadScript("https://cdn.jsdelivr.net/gh/openlayers/openlayers.github.io@master/en/v6.3.1/build/ol.js");
//  DaVinci.loadScript("https://cdn.polyfill.io/v2/polyfill.min.js?features=requestAnimationFrame,Element.prototype.classList,URL");
//  DaVinci.loadCSS("https://cdn.jsdelivr.net/gh/openlayers/openlayers.github.io@master/en/v6.3.1/css/ol.css");
}

(function (scriptElement) {
  DaVinci.addLoadEvent(function () {
    // Locate the parent of the script element to use as the map container
    var mapContainer = scriptElement.parentNode
    var scriptURL = scriptElement.src
    var serverURL = new URL(scriptURL).origin

    // Extract desired map information
    var dataCenter = scriptElement.getAttribute("data-center");
    var dataZoom = scriptElement.getAttribute("data-zoom");
    var dataBaseLayer = scriptElement.getAttribute("data-base-layer");
    var vizType = scriptElement.getAttribute("data-viz-type");
    var vizURL = scriptElement.getAttribute("data-viz-url");
    if (typeof vizURL === "undefined") {
      console.log("DaVinci: Dataset not defined in the script element. Please define the attribute 'data-dataset-name'");
      return;
    }
    // Initialize map information
    var latitude = 0.0, longitude = 0.0;
    var zoom = 1;
    var baseLayer = dataBaseLayer || "osm";

    if (dataCenter) {
      var parts = dataCenter.split(',');
      latitude = parseFloat(parts[0]);
      longitude = parseFloat(parts[1]);
    }
    if (dataZoom)
      zoom = parseInt(dataZoom);
    // Now, create the map and initialize as desired
    var view = new ol.View({
      center: ol.proj.fromLonLat([longitude, latitude]),
      zoom: zoom,
      minZoom: 0,
      maxZoom: 19
    });

    // Now initialize the base OSM layer and any user-selected data layers
    // Add the base OSM layer
    const OSMSource = new ol.source.OSM({ crossOrigin: null });
    const GoogleMapsSource = new ol.source.TileImage({
      wrapX: true,
      url: 'https://mt1.google.com/vt/lyrs=m&x={x}&y={y}&z={z}',
      attributions: 'Base layer by <a target="_blank" href="https://maps.google.com/">Google Maps</a>'
    });
    const GoogleSatelliteSource = new ol.source.TileImage({
      wrapX: true,
      url: 'https://mt1.google.com/vt/lyrs=s&hl=pl&&x={x}&y={y}&z={z}',
      attributions: 'Base layer by <a target="_blank" href="https://maps.google.com/">Google Maps</a>'
    });
    var source;
    switch (baseLayer) {
      case "osm": source = OSMSource; break;
      case "google-maps": source = GoogleMapsSource; break;
      case "google-satellite": source = GoogleSatelliteSource; break;
      case "none": source = null; break;
      default: source = OSMSource; break;
    }
    var layers = source? [new ol.layer.Tile({ source })] : [];
    layers.push(DaVinci.createDataLayer(vizType, vizURL, serverURL));

    // Now create the map
    var map = new ol.Map({
      target: mapContainer,
      layers: layers,
      view: view
    });
  })
})(document.currentScript);
