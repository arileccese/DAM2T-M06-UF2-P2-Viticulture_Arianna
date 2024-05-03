package manager;
import java.util.ArrayList;
import org.bson.Document;
import java.util.List;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.query.Query;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import model.Bodega;
import model.Campo;
import model.Entrada;
import model.Vid;
import utils.TipoVid;

public class Manager {
	private static Manager manager;
	private ArrayList<Entrada> entradas;
	private Session session;
	private Transaction tx;
	private Bodega b;
	private Campo c;
	private MongoDatabase database;
	private MongoCollection<Document> collection;


	private Manager () {
		this.entradas = new ArrayList<>();
	}
	
	public static Manager getInstance() {
		if (manager == null) {
			manager = new Manager();
		}
		return manager;
	}
	
	private void createSession() {
	    String uri = "mongodb://localhost:27017";
	    MongoClientURI mongoClientURI = new MongoClientURI(uri);
	    MongoClient mongoClient = new MongoClient(mongoClientURI);
	    database = mongoClient.getDatabase("winery");
	    
	    System.out.println("conectado a MONGODB");
	}


	public void init() {
		createSession();
		getEntrada();
		manageActions();
		// showAllCampos();
		session.close();
	}
	
	private void manageActions() {
		for (Entrada entrada : this.entradas) {
			try {
				System.out.println(entrada.getInstruccion());
				switch (entrada.getInstruccion().toUpperCase().split(" ")[0]) {
				
					case "B":
						addBodega(entrada.getInstruccion().split(" "));
						break;
						    
					case "C":
						addCampo(entrada.getInstruccion().split(" "));
					    break;
					    
					case "V":
						addVid(entrada.getInstruccion().split(" "));
					    break;
					    
					case "#":
						vendimia();
						break;
						
					default:
						System.out.println("Instruccion incorrecta");
				}
			} catch (HibernateException e) {
				e.printStackTrace();
				if (tx != null) {
					tx.rollback();
				}
			}
		}
	}

	
//	private void vendimia() {
//	    tx = session.beginTransaction();
//	    
//	    try {
//	        // Guardar la entidad b antes de la actualización
//	        session.save(b);
//	        
//	        // Ejecutar la consulta SQL para actualizar la tabla vid
//	        Query query = session.createSQLQuery("UPDATE vid SET campo_id = NULL WHERE bodega_id IS NOT NULL AND campo_id IS NOT NULL");
//	        int rowsAffected = query.executeUpdate();
//	        
//	        // Comprobar si se afectaron filas
//	        if (rowsAffected > 0) {
//	            System.out.println("Se actualizaron " + rowsAffected + " filas en la tabla vid.");
//	        } else {
//	            System.out.println("No se encontraron filas para actualizar en la tabla vid.");
//	        }
//	        
//	        // Confirmar la transacción
//	        tx.commit();
//	    } catch (Exception e) {
//	        // Si ocurre algún error, hacer rollback de la transacción
//	        if (tx != null) {
//	            tx.rollback();
//	        }
//	        e.printStackTrace();
//	    } finally {
//	        // Cerrar la sesión
//	        session.close();
//	    }
//	}

    private void vendimia() {
    this.b.getVids().addAll(this.c.getVids());
       
    tx = session.beginTransaction();
    session.save(b);
       
     tx.commit();
    }
 

//	private void addVid(String[] split) {
//		
////		Vid v = new Vid(TipoVid.valueOf(split[1].toUpperCase()), Integer.parseInt(split[2]));
////		tx = session.beginTransaction();
////		session.save(v);
////		
////		c.addVid(v);
////		session.save(c);
////		
////		tx.commit();
//		
//	}
	
	private void addVid(String[] split) {

	      System.out.println("vid añadido: " );
	        collection = database.getCollection("campo");
	        Document last = collection.find().sort(new Document("_id", -1)).first();
	       
	        collection = database.getCollection("vid");
	        Document document  = new Document().append("tipo_vid", split[1].toString())
	        .append("cantidad",Integer.parseInt(split[2])).append("campo", last);
	        collection.insertOne(document);
	}



	private void addCampo(String[] split) {
//		c = new Campo(b , split[1]);
//		tx = session.beginTransaction();
//		
//		int id = (Integer) session.save(c);
//			c = session.get(Campo.class, id);
//	
//		tx.commit();
//	

        Campo c = new Campo(b, split[1]);
        collection = database.getCollection("bodega");//añado la ultima bodega
        Document last = collection.find().sort(new Document("_id", -1)).first();
 
        Document campoDoc = new Document("nombre", c.getNombre()).append("Bodega", last);
        database.getCollection("campo").insertOne(campoDoc);
	}
	

	private void addBodega(String[] split) {
//		b = new Bodega(split[1]);
//		tx = session.beginTransaction();
//		
//		int id = (Integer) session.save(b);
//		b = session.get(Bodega.class, id);
//		
//		tx.commit();
		
	
        Bodega b = new Bodega(split[1]);
        
        Document bodegaDocumento = new Document("nombre", b.getNombre());
        database.getCollection("bodega").insertOne(bodegaDocumento);
        
	}

	private ArrayList <Entrada> getEntrada() {
//		tx = session.beginTransaction();
//		Query q = session.createQuery("select e from Entrada e");
//		this.entradas.addAll(q.list());
//		tx.commit();
		
		collection = database.getCollection("entrada");
		this.entradas =  new ArrayList<>();
		
		for (Document document : collection.find()) {
			Entrada input  = new Entrada();
			input.setInstruccion(document.getString("instruccion"));
			this.entradas.add(input); System.out.println(input);
		}
	
		return entradas;
	}

	private void showAllCampos() {
		tx = session.beginTransaction();
		Query q = session.createQuery("select c from Campo c");
		List<Campo> list = q.list();
		for (Campo c : list) {
			System.out.println(c);
		}
		tx.commit();
	}

	
}
