package com.billchau.springboot;

import org.springframework.data.jpa.repository.JpaRepository;

public interface WikimediaJPARepository extends JpaRepository<WikimediaData, Long> {
}
