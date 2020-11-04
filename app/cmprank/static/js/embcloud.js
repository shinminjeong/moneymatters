class EmbCloud {

  constructor(div_id, w, h) {
    this.div_id = div_id;
    this.width = w;
    this.height = h;
    console.log(this.width, this.height);
  }

  calculateScale(data) {
    var margin = 1;
    var groups = new Set();
    for (var key in data) {
      // save names of nodes
      var name = key.split("_");
      // var gname = name[0]+"-"+name[1];
      var gname = name[0];
      var year = name[1];
      var paperid = name[2];

      if (true) {
        // calculating boundary box
        this.bbox[0] = Math.min(this.bbox[0], data[key][0]-margin)
        this.bbox[1] = Math.max(this.bbox[1], data[key][0]+margin)
        this.bbox[2] = Math.min(this.bbox[2], data[key][1]-margin)
        this.bbox[3] = Math.max(this.bbox[3], data[key][1]+margin)
      }
    }
    this.bbox[4] = (this.bbox[0]+this.bbox[1])/2;
    this.bbox[5] = (this.bbox[2]+this.bbox[3])/2;

    var xd = this.bbox[1]-this.bbox[0],
        yd = this.bbox[3]-this.bbox[2];
    var xs = width/xd, ys = height/yd;
    var scale = Math.min(xs, ys);
    return scale;
  }

  drawCloud(data) {
    this.svg = d3.select("#"+this.div_id)
      .append('svg')
      .attr('width', this.width)
      .attr('height', this.height);
    this.draw_highlight = this.svg.append("g");
    this.draw_node = this.svg.append("g");
    this.bbox = [this.width,-this.width,this.height,-this.height,0,0];

    var scale = this.calculateScale(data);
    console.log("scale", scale, this.bbox);

    for (var name in data) {
      var newx = (data[name][0]-this.bbox[4])*scale+this.width/2,
          newy = (data[name][1]-this.bbox[5])*scale+this.height/2;

      // console.log(gname, year, paperid, data[key][0], data[key][1], newx, newy);
      var circle = this.draw_node.append("circle")
          .attr("id", name)
          .attr("class", "conference")
          .attr("r", 5)
          .attr("ox", data[name][0]).attr("oy", data[name][1])
          .attr("cx", newx).attr("cy", newy)
          .attr("fill", "#bbb")
          .on("click", function(d) { return showConfInfo(this.id) });
      var label = this.draw_node.append("text")
          .attr("id", name)
          .attr("class", "conference")
          .attr("x", newx+5).attr("y", newy+5)
          .text(name);
    }
  }

  selectConf(gid, clist){
    for (var c in clist) {
      try {
        var node = this.draw_node.select("circle#"+clist[c]).node();
        if (node == null) continue;
        var highlight = this.draw_highlight.append("circle")
          .attr("class", "highlight")
          .attr("r", 20)
          .attr("cx", node.getAttribute("cx"))
          .attr("cy", node.getAttribute("cy"))
          .attr("fill", d3.schemeSet1[gid])
          .attr("opacity", 0.2);
      } catch (err) {
        console.log(err);
      }
    }
  }

}
