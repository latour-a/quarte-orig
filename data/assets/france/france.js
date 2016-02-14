/*
  France Choropleth with Leaflet: https://choropleth.sylvaindurand.org
  Copyright 2015 Sylvain Durand – released under the MIT license.
*/

function france( map ) {


    this.init = function() {

                  // Download geospatial vector data and metadata
                  d3.json( '/data/assets/france/france.topojson', function ( e, json ) {
                    d3.csv( 'data/assets/france/data.csv', function ( e, data ) {

                      // Convert TopoJSON to GeoJSON
                      $.topo = topojson.feature( json, json.objects["can"] );

                      // Save events for faster layer replacements
                      $.save = { "moveend_idx":"", "zoomend_idx":"", "viewreset_idx":"" } ;
                      for ( var i in $.save ) {
                        for ( var j in map._events[i] ) {
                          $.save[i] = j;
                        }
                      }

                      // Store metadata
                      $.read( data );

                      // Display the first layer
                      $.load( arg[1] );

                      // Display search field
                      $.search();

                      // Layer control if two layers or more
                      arg[2] && $.select();

                    });
                  });

                }


    this.read = function( f ) {

                  // Create object for each feature
                  if ( !$.data ) {
                    $.data = {};
                    for ( var i in f ) {
                      $.data[ f[i].id ] = {};
                    }
                  }

                  // Attach metadata for each feature
                  for ( var j in f[0] ) {
                    if ( j != 'id' ) {
                      for ( var i in f ) {
                        if ( $.data[ f[i].id ] ) {
                          $.data[ f[i].id ][j] = f[i][j];
                        }
                      }
                    }
                  }

                }


    this.load = function ( arg ) {

                  // Store selected arguments
                  for ( var i in arg ) {
                    $[i] = arg[i];
                  }

                  // Download the selected stat
                  d3.csv( $.file, function ( err, csv ) {

                    // Store metadata
                    $.read( csv );

                    // (Re)draw the map
                    $.draw();

                    // Display legend
                    $.legend();

                    // Update an existing popup
                    d3.select(".leaflet-popup").remove();
                    $.i && $.popup( $.i );

                  });

                }


    this.draw = function() {

                  var
                        // Scale for density
                        alpha = d3.scale.log()
                                  .clamp( true )
                                  .domain( [1, 15000] )
                                  .range( [0, 1] ),

                        // Scale for color
                        color = d3.scale.linear()
                                         .clamp( true )
                                        .domain( $.domain )
                                         .range( $.range ),

                        // Define the renderer
                        canvas = L.canvas( { padding: .6 } );

                  // Remove previous canvas, layers and events
                  d3.select( ".leaflet-pane canvas" ).remove();
                  map._layers = {};
                  for ( var i in $.save ) {
                    var reset = map._events[i][ $.save[i] ];
                    map._events[i] = {};
                    map._events[i][ $.save[i] ] = reset ;
                  }

                  // Define new layer
                  map.addLayer(
                    new L.GeoJSON($.topo, {
                      renderer: canvas,
                      smoothFactor: .3,

                      // Show feature name and data on hover
                      onEachFeature: function ( feature, layer ) {
                        layer.on( {
                          mouseover: function() {
                                        d3.select( ".legend .value" )
                                          .attr( "value", (
                                            feature.id.slice( 2, 3 ) == "-" ? "Canton d"
                                            + ( ( /^[EÉAOUIY]/i ).test( $.data[ feature.id ].name ) ? "’" : "e " ) : "" )
                                            + $.data[ feature.id ].name + " : "
                                            + ( $.data[ feature.id ][ $.stat ] > 0 ? $.plus : "" )
                                            + $.data[ feature.id ][ $.stat ].replace( ".", "," ) + " "
                                            + $.unit
                                          )
                                      },

                           mouseout: function() {
                                        d3.select( ".legend .value" )
                                            .attr( "value", "" )
                                        }
                        } )
                      },

                      // Color depending on data
                      style: function( feature ) {
                               return {

                                 fillOpacity:
                                   $.data[ feature.id ] ? Math.max(alpha( $.data[ feature.id ].density ), .05 ) : .2,

                                 fillColor:
                                   color( $.data[ feature.id ] ? $.data[ feature.id ][$.stat ] : "#000"  ),

                                  stroke: 0

                               };
                             }
                    })
                  );

                }


  this.legend = function() {

                  // Delete existing legend to update it
                  d3.select( ".legend" ).remove();

                  var

                        // Legend size
                        width = 280,
                        padding = 10,

                        // Create legend control
                        div = d3.select( ".leaflet-bottom.leaflet-left" ).append( "div" )
                                  .attr( "class", "legend leaflet-control" ),

                        // Display title and unit
                        title = div.append( "div" )
                                     .attr( "class", "title" )
                                     .text( $.title[0]+' '+$.title[1] )
                                   .append( "span" )
                                     .text( " (" + $.unit + ")" ),

                        // Input where to display communes on hover
                        input = div.append( "input" )
                                     .attr( "class", "value" )
                                     .attr( "disabled", "" )
                                     .attr( "placeholder", "Survolez un territoire" ),

                        // Prepare linear scale and axis for gradient legend
                        x = d3.scale.linear()
                                    .domain( [$.domain[0], $.domain[$.domain.length-1]] )
                                     .range( [1, width - 2 * padding - 1] ),

                        canvas = div.append( "canvas" )
                                      .attr( "height", padding )
                                      .attr( "width", width - padding )
                                      .node().getContext( "2d" ),

                        gradient = canvas.createLinearGradient( 0, 0, width - 2 * padding, padding ),

                        stops = $.range.map( function( d, i ) { return { x: x( $.domain[i] ), color:d } } );

                  // Define color stops on the legend
                  for ( var s in stops ) {
                    gradient.addColorStop( stops[s].x/(width - 2 * padding - 1), stops[s].color );
                  }

                  // Draw the gradient rectangle
                  canvas.fillStyle = gradient;
                  canvas.fillRect( padding, 0, width - 2 * padding, padding );

                  // Draw horizontal axis
                  div.append( "svg" )
                       .attr( "width", width )
                       .attr( "height", 14 )
                     .append( "g" )
                       .attr( "class", "key" )
                       .attr( "transform", "translate( 10, 0 )" )
                       .call( d3.svg.axis()
                              .tickFormat( d3.format( $.plus + '.0f' ) )
                              .tickValues( $.domain )
                                .tickSize( 3 )
                                   .scale( x )
                          );
                }


  this.search = function() {

                  var
                        // Create the search control
                        div = d3.select( ".leaflet-top.leaflet-left" ).append( "div" )
                                  .attr( "class", "search leaflet-control" ),

                        // Create the search field
                        search = div.append( "input" )
                                      .attr( "type", "text" )
                                      .attr( "id", "field" )
                                      .attr( "placeholder", "Commune ou code postal" ),

                        // Initialize an empty list for autocompelte
                        list = [];

                  // Disable click propagation to make clicks work
                  L.DomEvent.disableClickPropagation( div.node() );
                  L.DomEvent.on( div.node(), 'mousewheel', L.DomEvent.stopPropagation );

                  // Create an autocomplete list
                  for ( var c in $.data ) {
                    $.data[c].x && list.push( $.data[c].name + " ( " + $.data[c].postcode + " )" );
                  }

                  // Use Awesomplete to autocomplete search input
                  new Awesomplete( document.getElementById( "field" ), { list: list, maxItems: 20 });

                  // Search for the commune when selected
                  search.on( 'awesomplete-selectcomplete', function() {

                    // Store the value before the loop
                    var value = search.node().value;

                    // Look for the right commune
                    for ( var i in $.data ) {
                      if ( value == $.data[i].name + " ( " + $.data[i].postcode + " )" ) {

                        // Open popup
                        $.popup( i );

                        // Clear the search field
                        search.node().value = '';

                        // Jump to the selected commune
                        map.flyTo( L.latLng( $.data[i].y, $.data[i].x ), 9 );

                        break;
                      }
                    }
                  });
                }


  this.select = function() {

                  // Create a control layer and disable click events
                  var control = L.control.layers().addTo( map );
                  control._onInputClick = function () {};

                  // Create radio for each available layer
                  for ( var i = 1; i < arg.length; i++ ) {

                    var

                          div = d3.select( ".leaflet-control-layers-base" ).append( "div" ),

                          input = div.append( "input" )
                                       .attr( "type", "radio" )
                                       .attr( "name", "select" )
                                       .attr( "id", "select" + i )
                                       .attr( i==1 ? "checked" : "unchecked", "" ),

                          span = div.append( "span" ).text( ' ' + arg[i].title[0] );

                    // Detect selected layer and load it
                    div.on( "click", function() {
                      d3.select( this ).select( "input" ).node().checked = true;
                      $.load( arg[ d3.select( this ).select( "input" ).attr( "id" ).slice( 6 ) ] );
                    });

                  }

                }


   this.popup = function( i ) {

                  // Prevent load() to reopen a closed popup
                  map.on( "popupclose", function() { delete $.i } );
                  $.i = i;

                  // Open a popup showing communes name and data
                  map.openPopup(L.popup().setLatLng( L.latLng( $.data[i].y, $.data[i].x ) )
                                         .setContent( '<b>' + $.data[i].name + '</b><br />'
                                                      + $.title[0] + ' : '
                                                      + ( $.data[i][ $.stat ] > 0 ? $.plus : "" )
                                                      + $.data[i][ $.stat ].replace( ".", "," ) + ' '
                                                      + $.unit) );

                }

    var $ = this, arg = arguments;
    $.init();

}
