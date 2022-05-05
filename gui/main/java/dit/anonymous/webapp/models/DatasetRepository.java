package dit.anonymous.webapp.models;

import org.springframework.data.repository.CrudRepository;

public interface DatasetRepository extends CrudRepository<Dataset, Integer>{
		
	Dataset findById(int id);

}
