package manager;
import java.util.ArrayList;
import java.util.List;


import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.query.Query;

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
		org.hibernate.SessionFactory sessionFactory = new Configuration().configure().buildSessionFactory();
    	session = sessionFactory.openSession();
	}

	public void init() {
		createSession();
		getEntrada();
		manageActions();
		showAllCampos();
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

	
	private void vendimia() {
	    tx = session.beginTransaction();
	    
	    try {
	        // Guardar la entidad b antes de la actualización
	        session.save(b);
	        
	        // Ejecutar la consulta SQL para actualizar la tabla vid
	        Query query = session.createSQLQuery("UPDATE vid SET campo_id = NULL WHERE bodega_id IS NOT NULL AND campo_id IS NOT NULL");
	        int rowsAffected = query.executeUpdate();
	        
	        // Comprobar si se afectaron filas
	        if (rowsAffected > 0) {
	            System.out.println("Se actualizaron " + rowsAffected + " filas en la tabla vid.");
	        } else {
	            System.out.println("No se encontraron filas para actualizar en la tabla vid.");
	        }
	        
	        // Confirmar la transacción
	        tx.commit();
	    } catch (Exception e) {
	        // Si ocurre algún error, hacer rollback de la transacción
	        if (tx != null) {
	            tx.rollback();
	        }
	        e.printStackTrace();
	    } finally {
	        // Cerrar la sesión
	        session.close();
	    }
	}


	private void addVid(String[] split) {
		Vid v = new Vid(TipoVid.valueOf(split[1].toUpperCase()), Integer.parseInt(split[2]));
		tx = session.beginTransaction();
		session.save(v);
		
		c.addVid(v);
		session.save(c);
		
		tx.commit();
		
	}

	private void addCampo(String[] split) {
		c = new Campo(b , split[1]);
		tx = session.beginTransaction();
		
		int id = (Integer) session.save(c);
		c = session.get(Campo.class, id);
		
		tx.commit();
	}

	private void addBodega(String[] split) {
		b = new Bodega(split[1]);
		tx = session.beginTransaction();
		
		int id = (Integer) session.save(b);
		b = session.get(Bodega.class, id);
		
		tx.commit();
		
	}

	private void getEntrada() {
		tx = session.beginTransaction();
		Query q = session.createQuery("select e from Entrada e");
		this.entradas.addAll(q.list());
		tx.commit();
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
