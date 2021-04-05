"use strict";

(function($) {
    function CircleProgress(config) {
        this.init(config);
    }
    CircleProgress.prototype = { 
		value: 10,
		total: 100,
		mstyle: 0,// 0<-> two circles the same thickness, 1<-> the top circle is in +4px thickness		
		title: "title",//title text
		caption: "caption",//caption text        		
        fgcolor: "#556b2f",//color of top circle
        bgcolor: "#eee",//color of background circle
        fill: false,//fill the background circle
        thik: 5,//radius of the circles
        tfontsize: 35,//title font size
		cfontsize: 15,//caption font size
		tfntname: "tahoma",//title font name
		cfntname: "arial",//caption font name
		tfntcolor: "#556b2f",//title font color
		cfntcolor: "#eee",//caption font color		
		tclass: "",//title css class name
		cclass: "",//caption css class name				
        animationstep: 0,//animation speed 0<-> no animation  
		rotateanimationstep: 0,//animate the circle rotation speed 0<-> no animation 
		shadow:0,//add shadow 0<-> no , shadow  1<->shadow
		//-------------------------------------- protected properties and methods --------------------------------------
        /**
         * @protected
         */
        constructor: CircleProgress,
        /**         
         * @protected
         * @type {number}
         */
        startdegree: 0,
		/**         
         * @protected
         * @type {number}
         */
        enddegree: 150,
		/**         
         * @protected
         * @type {number}
         */
        dimension: 0,
		/**         
         * @protected
         * @type {number}
         */
        sRotate: 0,
		/**         
         * @protected
         * @type {number}
         */
        eRotate: 0,	
		/**         
         * @protected
         * @type {number}
         */
        oThik: 10,
		/**         
         * @protected
         * @type {jQuery}
         */
		/**         
         * @protected
         * @type {number}
         */
        cX: 0,
		/**         
         * @protected
         * @type {number}
         */
        cY: 0,
        tblBG: null,
		/**         
         * @protected
         * @type {jQuery}
         */
        dvCaption: null,
		/**         
         * @protected
         * @type {jQuery}
         */
        dvTitle: null,
		/**         
         * @protected
         * @type {boolean}
         */
        forceRedraw: false,
		/**
         * @protected
         * @type {CanvasRenderingContext2D}
         */
        ctx: null,
		/**
         * @protected
         * @type {}
         */		 
		/**
         * @protected
         * @type {canvas}
         */
        canvas: null,
		/**
         * @protected
         * @type {}
         */ 
        hwndAnimation: null,
		/**
         * @protected
         * @type {}
         */
        hwndAnimationR: null,
		//getter and setter
        getValue: function() {
            return this.value;
        },
        setValue: function(newValue) {
			var self=this;
			this.value = newValue;	
			this.startdegree=0;//always 
			this.enddegree=(this.value/this.total)*360+this.startdegree;
			this.hwndAnimation=requestAnimationFrame(function () { self.animate(0); });	
        },
		getTotal: function() {
            return this.total;
        },
        setTotal: function(newValue) {
			this.total = newValue;
			var self=this;	
			this.startdegree=0;//always 
			this.enddegree=(this.value/this.total)*360+this.startdegree;
			this.hwndAnimation=requestAnimationFrame(function () { self.animate(0); });
        },
		getMstyle: function() {
            return this.mstyle;
        },

        setMstyle: function(newValue) {
			this.mstyle = newValue;
        },
		
		getTitle: function() {
            return this.title;
        },
        setTitle: function(newValue) {			
			this.title = newValue;
			$(this.dvTitle).text(this.title);				
        },
		getCaption: function() {
            return this.caption;
        },     
		setCaption: function(newValue) {
			this.caption = newValue;
			$(this.dvCaption).text(this.caption);
        },
		init: function(config) {
			var self=this;
            $.extend(this, config); 			
            this.initWidget();
			//bind resize event
			$.event.special.debouncedresize.threshold = 250;
			$(window).bind("debouncedresize", function() {				
				$(self.canvas)
					.attr('width', 1)
					.attr('height', 1);
				self.dimension = (self.el.parent().width() >= self.el.parent().height() ? self.el.parent().height() : self.el.parent().width());				
				self.calcCenterXY();
				$(self.canvas)
					.attr('width', self.dimension)
					.attr('height', self.dimension);			
			$(self.tblBG).remove();			
			self.drawTitle();
			self.justDraw=true;			
			$(self.canvas).css({'left': self.cX+'px'});
			$(self.canvas).css({'top': self.cY+'px'});			
			});
        },
		//init
        initWidget: function() {		    
			var self=this;
			this.oThik=this.thik;
			self.dimension = (this.el.parent().width() >= this.el.parent().height() ? this.el.parent().height() : this.el.parent().width());
			this.calcCenterXY();
			this.createCanvas();
			this.drawTitle();
			this.startdegree=0;//always 
			this.enddegree=(this.value/this.total)*360+this.startdegree; 									           		
			this.hwndAnimation=requestAnimationFrame(function () { self.animate(0); });			
        },
		calcCenterXY: function() {
				this.cX=(this.el.parent().width()-this.dimension)/2;
				this.cY=(this.el.parent().height()-this.dimension)/2;
				this.thik=this.oThik*this.dimension*0.003100;
				console.log('parent.width:'+this.el.parent().width()+' , dimension:'+this.dimension+' , cX:'+this.cX);
		},
		createCanvas: function() {			     
				var size = this.dimension;
				this.canvas = $('<canvas style="position:relative;display:block;"></canvas>').attr({
                    width: size,
                    height: size
                }).appendTo(this.el)[0];				
				$(this.canvas).css({'left': this.cX+'px'});
				$(this.canvas).css({'top': this.cY+'px'});				
				this.ctx = this.canvas.getContext('2d');				
		},
		drawBGCircle: function (){
			    var cntrX=Math.floor(this.dimension/2);
				var cntrY=Math.floor(this.dimension/2);
				var Radius=Math.floor(this.dimension/2);
				Radius-=this.thik/1.5;
				this.ctx.lineWidth = this.thik;							
				if(Radius<=0) Radius=1;
				this.ctx.shadowBlur = 0;
				this.ctx.shadowColor="rgba(0,0,0,.2)";
				this.ctx.shadowOffsetX = 0;
				this.ctx.shadowOffsetY = 0;
				this.ctx.strokeStyle = this.bgcolor;
				this.ctx.beginPath();
				this.ctx.arc(cntrX, cntrY, Radius, 0, 2*Math.PI, true); 								
				this.ctx.stroke();				
			},
		drawFrontCircle: function (sAngle,eAngle){			    
			    var cntrX=Math.floor(this.dimension/2);
				var cntrY=Math.floor(this.dimension/2);
				var Radius=Math.floor(this.dimension/2);				
				Radius-=this.thik/1.5;
				this.ctx.clearRect(0,0,this.dimension,this.dimension);
				this.drawBGCircle();
				this.ctx.lineWidth = this.thik;
				if(this.mstyle==1){
				  this.ctx.lineWidth = this.thik+(this.thik*0.666666);
				  Radius-=this.thik/2;
				}
				else if(this.mstyle==2){
				  this.ctx.lineWidth = this.thik+(this.thik*0.666666);
				  Radius-=this.thik*2;
				}
				if(Radius<=0) Radius=1;
				if(this.shadow==1){
					this.ctx.shadowBlur = 2;
					this.ctx.shadowColor="rgba(0,0,0,0.2)";
					this.ctx.shadowOffsetX = 2;
					this.ctx.shadowOffsetY = 2;
				}	
				this.ctx.strokeStyle = this.fgcolor;				
				this.ctx.beginPath();
				this.ctx.arc(cntrX, cntrY, Radius, sAngle, eAngle, false); 								
				this.ctx.stroke();						
			},
		animate: function (percent) {
		        var self=this;
				var sAngle = this.startdegree;					
				if(this.animationstep>0){
					this.drawFrontCircle(this.getRadian(sAngle),this.getRadian(percent));
					percent+=this.animationstep;
					if(percent<=this.enddegree){
						this.hwndAnimation=requestAnimationFrame(function () {
							self.animate(percent);
						});
					}
					else{
						this.sRotate=this.startdegree;
					    this.eRotate=this.enddegree;					    
						this.hwndAnimationR=requestAnimationFrame(function () {
							self.animateRotation();
						});
					}
				}
				else{				    
					this.drawFrontCircle(this.getRadian(sAngle),this.getRadian(this.enddegree));
					if(this.rotateanimationstep>0){
						this.sRotate=this.startdegree;
					    this.eRotate=this.enddegree;					    
						this.hwndAnimationR=requestAnimationFrame(function () {							
							self.animateRotation();
						});
					}
				}
			},
		animateRotation: function () {
			 var self=this;
			 if(this.forceRedraw) {			     
				 this.sRotate=this.startdegree;
				 this.eRotate=this.enddegree;
				 drawFrontCircle(getRadian(this.startdegree),getRadian(this.enddegree));
				 this.forceRedraw=false;
			}	
			 if(this.sRotate>=(360-this.startdegree)) {
				this.sRotate=this.startdegree;
				this.eRotate=this.enddegree;
			}
			 this.sRotate+=this.rotateanimationstep;
			 this.eRotate+=this.rotateanimationstep;			 
			 this.drawFrontCircle(this.getRadian(this.sRotate),this.getRadian(this.eRotate));
			 this.hwndAnimationR=requestAnimationFrame(function () {
							self.animateRotation();
                    });			
			},
		drawTitle: function (){				
				var tblTitle = $('<table></table>').appendTo(this.el)[0];
				this.tblBG=tblTitle;
				var trTitle = $('<tr></tr>').appendTo(tblTitle).get(0);
				var tdTitle = $('<td></td>').appendTo(trTitle).get(0);
				var fntTitleSize=this.dimension*0.2800;
				var fntCaptionSize=this.dimension*0.0620;				
				this.dvTitle = $('<div></div>').appendTo(tdTitle).get(0);
				this.dvCaption = $('<div></div>').appendTo(tdTitle).get(0);
				var topAlign='-'+this.dimension.toString()+'px';				
				$(tblTitle).css({'width': this.dimension.toString()+'px'});
				$(tblTitle).css({'height': this.dimension.toString()+'px'});
				$(tblTitle).css({'vertical-align': 'middle'});
				$(tblTitle).css({'position': 'relative'});
				$(tblTitle).css({'margin-top': topAlign});				
				$(tblTitle).css({'font': '14px Tahoma, Helvetica, sans-serif'});				
				$(tblTitle).css({'text-align': 'center'})
				$(tblTitle).css({'left': this.cX+'px'})
				$(tblTitle).css({'top': this.cY+'px'});
				this.setTitle(this.title);
				this.setCaption(this.caption);							
				if(this.tid!="") {
					$(this.dvTitle).attr({id:this.tid});
					$(tdTitle).css({'cursor': 'pointer'});
				} 				
				if(this.tclass!="") $(this.dvTitle).addClass(this.tclass);
				else{						
					$(this.dvTitle).css({'font-size': fntTitleSize+'px'});	
					$(this.dvTitle).css({'color': this.tfntcolor});
				}				
				if(this.cclass!="") $(dvCaption).addClass(this.cclass);
				else{				
					$(this.dvCaption).css({'font-size': fntCaptionSize+'px'});	
					$(this.dvCaption).css({'color': this.cfntcolor});
				}				
			},
		getRadian: function (degrees) {
			    //correct the offset 0<->at 3 o'Clock (-90 deg)
				degrees-=90;
                return degrees * Math.PI / 180;
            }
    };
    //-------------------------------------------- Initiating jQuery plugin --------------------------------------------
    $.circleProgress = {
        // Default options (you may override them)
        defaults: CircleProgress.prototype
    };
    //-------------------------------------------------------------------------------------------------------------------
    $.fn.circleProgress = function(configOrCommand, commandArgument) {
        var dataName = 'circle-progress',
            firstInstance = this.data(dataName);
		if (configOrCommand == 'widget') {
            if (!firstInstance)
                throw Error('Calling "widget" method on not initialized instance is forbidden');
            return firstInstance.canvas;
        }
        if (configOrCommand == 'title') {
            if (!firstInstance)
                throw Error('Calling "title" method on not initialized instance is forbidden');
            if (typeof commandArgument == 'undefined') {
                return firstInstance.getTitle();
            } else {
                var newValue = arguments[1];
                return this.each(function() {
					$(this).data(dataName).setTitle(newValue);                   
                });
            }
        }
		else if (configOrCommand == 'caption') {
            if (!firstInstance)
                throw Error('Calling "caption" method on not initialized instance is forbidden');
            if (typeof commandArgument == 'undefined') {
                return firstInstance.getCaption();
            } else {
                var newValue = arguments[1];
                return this.each(function() {
					$(this).data(dataName).setCaption(newValue);                   
                });
            }
        }
		else if (configOrCommand == 'value') {
            if (!firstInstance)
                throw Error('Calling "value" method on not initialized instance is forbidden');
            if (typeof commandArgument == 'undefined') {
                return firstInstance.getValue();
            } else {
                var newValue = arguments[1];
                return this.each(function() {
					$(this).data(dataName).setValue(newValue);                   
                });
            }
        }
		else if (configOrCommand == 'total') {
            if (!firstInstance)
                throw Error('Calling "total" method on not initialized instance is forbidden');
            if (typeof commandArgument == 'undefined') {
                return firstInstance.getTotal();
            } else {
                var newValue = arguments[1];
                return this.each(function() {
					$(this).data(dataName).setTotal(newValue);                   
                });
            }
        }
        return this.each(function() {
            var el = $(this),
                instance = el.data(dataName),
                config = $.isPlainObject(configOrCommand) ? configOrCommand : {};

            if (instance) {
                instance.init(config);
            } else {
                var initialConfig = $.extend({}, el.data());
                if (typeof initialConfig.fill == 'string')
                    initialConfig.fill = JSON.parse(initialConfig.fill);                
                config = $.extend(initialConfig, config);
                config.el = el;
                instance = new CircleProgress(config);
                el.data(dataName, instance);
            }
        });
    };
})(jQuery);
