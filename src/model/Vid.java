package model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import utils.TipoVid;

@Entity
@Table(name= "vid")
public class Vid {
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	@Column(name = "id", unique = true, nullable = true)
	private int id;
	
	@Column(name = "tipo_vid", nullable = true)
	private TipoVid vid;
	
	@Column(name = "cantidad", nullable = true)
	private int cantidad;
	
	@Column(name = "bodega_id" , nullable = true)
	private long bodega_id;
	
	@Column(name = "campo_id", nullable = true)
	private long campo_id;
	
	public long getBodega_id() {
		return bodega_id;
	}

	public void setBodega_id(long bodega_id) {
		this.bodega_id = bodega_id;
	}

	public long getCampo_id() {
		return campo_id;
	}

	public void setCampo_id(long campo_id) {
		this.campo_id = campo_id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public void setVid(TipoVid vid) {
		this.vid = vid;
	}

	public void setCantidad(int cantidad) {
		this.cantidad = cantidad;
	}

	public Vid() {}
		
	public Vid(TipoVid vid, int cantidad) {
		this.vid = vid;
		this.cantidad = cantidad;
	}
	public int getId() {
		return this.id;
	}
	public TipoVid getVid() {
		return vid;
	}
	public int getCantidad() {
		return cantidad;
	}
	@Override
	public String toString() {
		return "Vid [vid=" + (vid.equals("0") ? "blanca" : "negra")  + ", cantidad=" + cantidad + "]";
	}
}
