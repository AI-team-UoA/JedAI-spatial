
package dit.anonymous.webapp.models;

import org.springframework.data.repository.CrudRepository;


public interface SimilarityJoinRepository extends CrudRepository<SimilarityMethod, Integer>{
	
	SimilarityMethod findById(int id);

}
